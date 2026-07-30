//! Persistent completed-transaction coordination for retry-safe HTTP forwarding.

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Notify};

/// Result of claiming a transaction identifier.
#[derive(Debug)]
pub enum CompletionClaim {
    /// The caller owns execution and must eventually record or abandon the result.
    Execute,
    /// A retained response can be replayed without forwarding upstream again.
    Replay(Bytes),
    /// The transaction completed but its response was intentionally not retained.
    Tombstone,
    /// Another task is executing the transaction; wait and claim again after notification.
    Wait(Arc<Notify>),
    /// The process-wide active transaction limit is currently exhausted.
    Capacity,
}

/// Existing completed state used by streamed opening deduplication.
#[derive(Debug, Clone)]
pub enum CompletionLookup {
    /// A complete response was retained.
    Response(Bytes),
    /// The request completed but no response body is retained.
    Tombstone,
    /// The transaction is currently executing through another request mode.
    InFlight,
}

#[derive(Debug)]
enum Entry {
    InFlight(Arc<Notify>),
    Complete {
        response: Option<Bytes>,
        expires_at_unix_seconds: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedCompletion {
    schema: String,
    transaction_id: String,
    expires_at_unix_seconds: u64,
    response_base64: Option<String>,
}

/// Bounded persistent completion store shared by atomic and streamed bridge paths.
#[derive(Debug)]
pub struct CompletionStore {
    directory: PathBuf,
    retention: Duration,
    max_response_bytes: usize,
    max_completed_entries: usize,
    max_in_flight: usize,
    entries: Mutex<HashMap<[u8; 16], Entry>>,
}

impl CompletionStore {
    /// Open a completion directory and recover unexpired tombstones/responses.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created or enumerated.
    pub fn open(
        directory: PathBuf,
        retention: Duration,
        max_response_bytes: usize,
        max_completed_entries: usize,
        max_in_flight: usize,
    ) -> Result<Arc<Self>> {
        fs::create_dir_all(&directory)
            .with_context(|| format!("create completion directory {}", directory.display()))?;
        let now = now_unix_seconds()?;
        let mut entries = HashMap::new();
        for item in fs::read_dir(&directory)
            .with_context(|| format!("read completion directory {}", directory.display()))?
        {
            let item = match item {
                Ok(item) => item,
                Err(error) => {
                    tracing::warn!(%error, "ignored unreadable completion directory entry");
                    continue;
                }
            };
            let path = item.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            match load_entry(&path) {
                Ok((id, response, expires_at)) if expires_at > now => {
                    entries.insert(
                        id,
                        Entry::Complete {
                            response,
                            expires_at_unix_seconds: expires_at,
                        },
                    );
                }
                Ok(_) => {
                    let _ = fs::remove_file(&path);
                }
                Err(error) => {
                    tracing::warn!(%error, path = %path.display(), "ignored invalid completion record");
                }
            }
        }
        trim_loaded_entries(
            &directory,
            &mut entries,
            max_completed_entries.max(1),
        );
        Ok(Arc::new(Self {
            directory,
            retention,
            max_response_bytes,
            max_completed_entries: max_completed_entries.max(1),
            max_in_flight: max_in_flight.max(1),
            entries: Mutex::new(entries),
        }))
    }

    /// Claim a transaction for execution, replay, waiting, or capacity rejection.
    pub async fn claim(&self, id: [u8; 16]) -> CompletionClaim {
        let mut entries = self.entries.lock().await;
        self.prune_locked(&mut entries);
        match entries.get(&id) {
            Some(Entry::InFlight(notify)) => CompletionClaim::Wait(Arc::clone(notify)),
            Some(Entry::Complete {
                response: Some(response),
                ..
            }) => CompletionClaim::Replay(response.clone()),
            Some(Entry::Complete { response: None, .. }) => CompletionClaim::Tombstone,
            None => {
                let active = entries
                    .values()
                    .filter(|entry| matches!(entry, Entry::InFlight(_)))
                    .count();
                if active >= self.max_in_flight {
                    return CompletionClaim::Capacity;
                }
                entries.insert(id, Entry::InFlight(Arc::new(Notify::new())));
                CompletionClaim::Execute
            }
        }
    }

    /// Look up completion state without claiming an absent identifier.
    pub async fn lookup(&self, id: [u8; 16]) -> Option<CompletionLookup> {
        let mut entries = self.entries.lock().await;
        self.prune_locked(&mut entries);
        entries.get(&id).map(|entry| match entry {
            Entry::InFlight(_) => CompletionLookup::InFlight,
            Entry::Complete {
                response: Some(response),
                ..
            } => CompletionLookup::Response(response.clone()),
            Entry::Complete { response: None, .. } => CompletionLookup::Tombstone,
        })
    }

    /// Record completion and wake duplicate waiters.
    ///
    /// Responses larger than the configured retention threshold become tombstones.
    /// In-memory protection is established before disk persistence is attempted.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable record cannot be written.
    pub async fn record(&self, id: [u8; 16], response: Option<Bytes>) -> Result<()> {
        let response = response.filter(|bytes| bytes.len() <= self.max_response_bytes);
        let expires_at = now_unix_seconds()?.saturating_add(self.retention.as_secs());
        let mut entries = self.entries.lock().await;
        self.prune_locked(&mut entries);
        let notify = match entries.remove(&id) {
            Some(Entry::InFlight(notify)) => Some(notify),
            _ => None,
        };
        entries.insert(
            id,
            Entry::Complete {
                response: response.clone(),
                expires_at_unix_seconds: expires_at,
            },
        );
        let evicted = self.enforce_completed_limit_locked(&mut entries, id);
        drop(entries);
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
        persist_entry(&self.directory, id, response.as_ref(), expires_at)?;
        for evicted_id in evicted {
            let _ = fs::remove_file(self.file_path(evicted_id));
        }
        Ok(())
    }

    /// Remove an in-flight claim when execution is known not to have reached the upstream.
    pub async fn abandon(&self, id: [u8; 16]) {
        let notify = match self.entries.lock().await.remove(&id) {
            Some(Entry::InFlight(notify)) => Some(notify),
            _ => None,
        };
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    fn prune_locked(&self, entries: &mut HashMap<[u8; 16], Entry>) {
        let now = now_unix_seconds().unwrap_or(u64::MAX);
        let expired = entries
            .iter()
            .filter_map(|(id, entry)| match entry {
                Entry::Complete {
                    expires_at_unix_seconds,
                    ..
                } if *expires_at_unix_seconds <= now => Some(*id),
                _ => None,
            })
            .collect::<Vec<_>>();
        for id in expired {
            entries.remove(&id);
            let _ = fs::remove_file(self.file_path(id));
        }
    }

    fn enforce_completed_limit_locked(
        &self,
        entries: &mut HashMap<[u8; 16], Entry>,
        protected_id: [u8; 16],
    ) -> Vec<[u8; 16]> {
        let mut evicted = Vec::new();
        while completed_count(entries) > self.max_completed_entries {
            let oldest = entries
                .iter()
                .filter_map(|(id, entry)| match entry {
                    Entry::Complete {
                        expires_at_unix_seconds,
                        ..
                    } if *id != protected_id => Some((*id, *expires_at_unix_seconds)),
                    _ => None,
                })
                .min_by_key(|(_, expires_at)| *expires_at)
                .map(|(id, _)| id);
            let Some(id) = oldest else {
                break;
            };
            entries.remove(&id);
            evicted.push(id);
        }
        evicted
    }

    fn file_path(&self, id: [u8; 16]) -> PathBuf {
        self.directory.join(format!("{}.json", hex_transaction(id)))
    }
}

fn completed_count(entries: &HashMap<[u8; 16], Entry>) -> usize {
    entries
        .values()
        .filter(|entry| matches!(entry, Entry::Complete { .. }))
        .count()
}

fn trim_loaded_entries(
    directory: &Path,
    entries: &mut HashMap<[u8; 16], Entry>,
    max_completed_entries: usize,
) {
    while completed_count(entries) > max_completed_entries {
        let oldest = entries
            .iter()
            .filter_map(|(id, entry)| match entry {
                Entry::Complete {
                    expires_at_unix_seconds,
                    ..
                } => Some((*id, *expires_at_unix_seconds)),
                Entry::InFlight(_) => None,
            })
            .min_by_key(|(_, expires_at)| *expires_at)
            .map(|(id, _)| id);
        let Some(id) = oldest else {
            break;
        };
        entries.remove(&id);
        let _ = fs::remove_file(directory.join(format!("{}.json", hex_transaction(id))));
    }
}

fn persist_entry(
    directory: &Path,
    id: [u8; 16],
    response: Option<&Bytes>,
    expires_at: u64,
) -> Result<()> {
    let record = PersistedCompletion {
        schema: "org.veilidhttp.completed/v1".to_owned(),
        transaction_id: hex_transaction(id),
        expires_at_unix_seconds: expires_at,
        response_base64: response.map(|bytes| URL_SAFE_NO_PAD.encode(bytes)),
    };
    let destination = directory.join(format!("{}.json", record.transaction_id));
    let temporary = destination.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(&record)?)
        .with_context(|| format!("write completion record {}", temporary.display()))?;
    fs::rename(&temporary, &destination)
        .with_context(|| format!("replace completion record {}", destination.display()))?;
    Ok(())
}

fn load_entry(path: &Path) -> Result<([u8; 16], Option<Bytes>, u64)> {
    let record: PersistedCompletion = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("read completion record {}", path.display()))?,
    )?;
    if record.schema != "org.veilidhttp.completed/v1" {
        anyhow::bail!("unsupported completion schema");
    }
    let id = parse_hex_transaction(&record.transaction_id)?;
    let response = record
        .response_base64
        .map(|value| URL_SAFE_NO_PAD.decode(value.as_bytes()).map(Bytes::from))
        .transpose()
        .context("decode retained atomic response")?;
    Ok((id, response, record.expires_at_unix_seconds))
}

fn now_unix_seconds() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs())
}

fn hex_transaction(id: [u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_hex_transaction(value: &str) -> Result<[u8; 16]> {
    if value.len() != 32 {
        anyhow::bail!("transaction identifier must contain 32 hexadecimal characters");
    }
    let mut id = [0_u8; 16];
    for (index, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .context("invalid hexadecimal transaction identifier")?;
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporary_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "veilid-http-completion-{name}-{}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn retained_response_survives_reopen() {
        let directory = temporary_directory("reopen");
        let _ = fs::remove_dir_all(&directory);
        let id = [7_u8; 16];
        let store = CompletionStore::open(
            directory.clone(),
            Duration::from_secs(60),
            1024,
            16,
            4,
        )
        .unwrap();
        assert!(matches!(store.claim(id).await, CompletionClaim::Execute));
        store
            .record(id, Some(Bytes::from_static(b"response")))
            .await
            .unwrap();
        drop(store);
        let reopened = CompletionStore::open(
            directory.clone(),
            Duration::from_secs(60),
            1024,
            16,
            4,
        )
        .unwrap();
        assert!(matches!(
            reopened.lookup(id).await,
            Some(CompletionLookup::Response(ref bytes)) if bytes.as_ref() == b"response"
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn oversized_response_becomes_tombstone() {
        let directory = temporary_directory("tombstone");
        let _ = fs::remove_dir_all(&directory);
        let id = [9_u8; 16];
        let store = CompletionStore::open(
            directory.clone(),
            Duration::from_secs(60),
            4,
            16,
            4,
        )
        .unwrap();
        assert!(matches!(store.claim(id).await, CompletionClaim::Execute));
        store
            .record(id, Some(Bytes::from_static(b"too-large")))
            .await
            .unwrap();
        assert!(matches!(
            store.lookup(id).await,
            Some(CompletionLookup::Tombstone)
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn active_capacity_never_evicts_live_execution() {
        let directory = temporary_directory("capacity");
        let _ = fs::remove_dir_all(&directory);
        let store = CompletionStore::open(
            directory.clone(),
            Duration::from_secs(60),
            1024,
            16,
            1,
        )
        .unwrap();
        assert!(matches!(store.claim([1; 16]).await, CompletionClaim::Execute));
        assert!(matches!(store.claim([2; 16]).await, CompletionClaim::Capacity));
        store.abandon([1; 16]).await;
        assert!(matches!(store.claim([2; 16]).await, CompletionClaim::Execute));
        fs::remove_dir_all(directory).unwrap();
    }
}
