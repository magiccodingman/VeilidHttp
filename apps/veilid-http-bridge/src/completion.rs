//! Persistent completed-transaction coordination for retry-safe HTTP forwarding.

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Notify};

/// Default number of HTTP transactions allowed to execute concurrently in the bridge.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 128;

/// Result of claiming a transaction identifier.
#[derive(Debug)]
pub enum CompletionClaim {
    /// The caller owns execution and must eventually record or abandon the result.
    Execute,
    /// A retained response can be replayed without forwarding upstream again.
    Replay(Bytes),
    /// The transaction completed, may have completed before a crash, or could not be
    /// durably claimed. It must not be automatically forwarded again.
    Tombstone,
    /// The same transaction is already executing and must complete before retrying.
    WaitDuplicate(Arc<Notify>),
    /// The process-wide execution bound is full and must free capacity before retrying.
    WaitCapacity(Arc<Notify>),
}

/// Existing completed state used by streamed opening deduplication.
#[derive(Debug, Clone)]
pub enum CompletionLookup {
    /// A complete response was retained.
    Response(Bytes),
    /// The request completed or its result became indeterminate across a restart.
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
    /// A durable pre-execution claim. After a process restart this is treated as an
    /// indeterminate tombstone: the upstream may or may not have executed it, so the
    /// bridge must never forward the same transaction identifier automatically.
    #[serde(default)]
    in_flight: bool,
}

/// Bounded persistent completion store shared by atomic and streamed bridge paths.
#[derive(Debug)]
pub struct CompletionStore {
    directory: PathBuf,
    retention: Duration,
    max_response_bytes: usize,
    max_completed_entries: usize,
    max_in_flight: usize,
    capacity_notify: Arc<Notify>,
    entries: Mutex<HashMap<[u8; 16], Entry>>,
}

impl CompletionStore {
    /// Open a completion directory with the default active transaction limit.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created or enumerated.
    pub fn open(
        directory: PathBuf,
        retention: Duration,
        max_response_bytes: usize,
        max_completed_entries: usize,
    ) -> Result<Arc<Self>> {
        Self::open_with_active_limit(
            directory,
            retention,
            max_response_bytes,
            max_completed_entries,
            DEFAULT_MAX_IN_FLIGHT,
        )
    }

    /// Open a completion directory with an explicit process-wide execution limit.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created or enumerated.
    pub fn open_with_active_limit(
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
                Ok((id, response, expires_at, in_flight)) if expires_at > now => {
                    let response = if in_flight { None } else { response };
                    entries.insert(
                        id,
                        Entry::Complete {
                            response: response.clone(),
                            expires_at_unix_seconds: expires_at,
                        },
                    );
                    if in_flight {
                        if let Err(error) =
                            persist_entry(&directory, id, response.as_ref(), expires_at, false)
                        {
                            tracing::warn!(
                                %error,
                                path = %path.display(),
                                "could not normalize recovered in-flight claim to tombstone"
                            );
                        }
                    }
                }
                Ok(_) => {
                    let _ = fs::remove_file(&path);
                }
                Err(error) => {
                    tracing::warn!(%error, path = %path.display(), "ignored invalid completion record");
                }
            }
        }
        trim_loaded_entries(&directory, &mut entries, max_completed_entries.max(1));
        Ok(Arc::new(Self {
            directory,
            retention,
            max_response_bytes,
            max_completed_entries: max_completed_entries.max(1),
            max_in_flight: max_in_flight.max(1),
            capacity_notify: Arc::new(Notify::new()),
            entries: Mutex::new(entries),
        }))
    }

    /// Claim a transaction for execution, replay, or waiting.
    ///
    /// A new claim is written to durable storage before `Execute` is returned. If that
    /// write fails, the transaction becomes an in-memory tombstone and is not forwarded.
    /// This deliberately prefers zero executions over the possibility of two.
    pub async fn claim(&self, id: [u8; 16]) -> CompletionClaim {
        let mut entries = self.entries.lock().await;
        self.prune_locked(&mut entries);
        match entries.get(&id) {
            Some(Entry::InFlight(notify)) => CompletionClaim::WaitDuplicate(Arc::clone(notify)),
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
                    return CompletionClaim::WaitCapacity(Arc::clone(&self.capacity_notify));
                }

                let expires_at = match now_unix_seconds() {
                    Ok(now) => now.saturating_add(self.retention.as_secs()),
                    Err(error) => {
                        tracing::error!(%error, transaction = %hex_transaction(id), "refused transaction because its durable claim timestamp could not be created");
                        return CompletionClaim::Tombstone;
                    }
                };
                if let Err(error) = persist_entry(&self.directory, id, None, expires_at, true) {
                    tracing::error!(
                        %error,
                        transaction = %hex_transaction(id),
                        "refused transaction because its pre-execution claim could not be persisted"
                    );
                    entries.insert(
                        id,
                        Entry::Complete {
                            response: None,
                            expires_at_unix_seconds: expires_at,
                        },
                    );
                    let evicted = self.enforce_completed_limit_locked(&mut entries, id);
                    drop(entries);
                    for evicted_id in evicted {
                        let _ = fs::remove_file(self.file_path(evicted_id));
                    }
                    return CompletionClaim::Tombstone;
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

    /// Record completion and wake duplicate/capacity waiters.
    ///
    /// Responses larger than the configured retention threshold become tombstones.
    /// In-memory protection is established before disk persistence is attempted. If the
    /// final write fails, the earlier durable in-flight claim remains and is recovered as
    /// an indeterminate tombstone after restart.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable completion record cannot be written.
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
            notify.notify_one();
        }
        self.capacity_notify.notify_one();
        persist_entry(&self.directory, id, response.as_ref(), expires_at, false)?;
        for evicted_id in evicted {
            let _ = fs::remove_file(self.file_path(evicted_id));
        }
        Ok(())
    }

    /// Remove an in-flight claim when execution is known not to have reached the upstream.
    pub async fn abandon(&self, id: [u8; 16]) {
        let notify = {
            let mut entries = self.entries.lock().await;
            if matches!(entries.get(&id), Some(Entry::InFlight(_))) {
                match entries.remove(&id) {
                    Some(Entry::InFlight(notify)) => Some(notify),
                    _ => None,
                }
            } else {
                None
            }
        };
        if notify.is_some() {
            match fs::remove_file(self.file_path(id)) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(
                    %error,
                    transaction = %hex_transaction(id),
                    "could not remove abandoned durable transaction claim"
                ),
            }
        }
        if let Some(notify) = notify {
            notify.notify_one();
        }
        self.capacity_notify.notify_one();
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
    in_flight: bool,
) -> Result<()> {
    let record = PersistedCompletion {
        schema: "org.veilidhttp.completed/v1".to_owned(),
        transaction_id: hex_transaction(id),
        expires_at_unix_seconds: expires_at,
        response_base64: response.map(|bytes| URL_SAFE_NO_PAD.encode(bytes)),
        in_flight,
    };
    let destination = directory.join(format!("{}.json", record.transaction_id));
    let temporary = destination.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(&record)?)
        .with_context(|| format!("write completion record {}", temporary.display()))?;
    fs::rename(&temporary, &destination)
        .with_context(|| format!("replace completion record {}", destination.display()))?;
    Ok(())
}

fn load_entry(path: &Path) -> Result<([u8; 16], Option<Bytes>, u64, bool)> {
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
    Ok((
        id,
        response,
        record.expires_at_unix_seconds,
        record.in_flight,
    ))
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
        let store =
            CompletionStore::open(directory.clone(), Duration::from_secs(60), 1024, 16).unwrap();
        assert!(matches!(store.claim(id).await, CompletionClaim::Execute));
        store
            .record(id, Some(Bytes::from_static(b"response")))
            .await
            .unwrap();
        drop(store);
        let reopened =
            CompletionStore::open(directory.clone(), Duration::from_secs(60), 1024, 16).unwrap();
        assert!(matches!(
            reopened.lookup(id).await,
            Some(CompletionLookup::Response(ref bytes)) if bytes.as_ref() == b"response"
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn durable_claim_becomes_tombstone_after_restart() {
        let directory = temporary_directory("inflight-restart");
        let _ = fs::remove_dir_all(&directory);
        let id = [8_u8; 16];
        let store =
            CompletionStore::open(directory.clone(), Duration::from_secs(60), 1024, 16).unwrap();
        assert!(matches!(store.claim(id).await, CompletionClaim::Execute));
        assert!(
            directory
                .join(format!("{}.json", hex_transaction(id)))
                .is_file()
        );
        drop(store);

        let reopened =
            CompletionStore::open(directory.clone(), Duration::from_secs(60), 1024, 16).unwrap();
        assert!(matches!(
            reopened.claim(id).await,
            CompletionClaim::Tombstone
        ));
        assert!(matches!(
            reopened.lookup(id).await,
            Some(CompletionLookup::Tombstone)
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn abandoned_claim_can_execute_again() {
        let directory = temporary_directory("abandon");
        let _ = fs::remove_dir_all(&directory);
        let id = [6_u8; 16];
        let store =
            CompletionStore::open(directory.clone(), Duration::from_secs(60), 1024, 16).unwrap();
        assert!(matches!(store.claim(id).await, CompletionClaim::Execute));
        store.abandon(id).await;
        assert!(matches!(store.claim(id).await, CompletionClaim::Execute));
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn oversized_response_becomes_tombstone() {
        let directory = temporary_directory("tombstone");
        let _ = fs::remove_dir_all(&directory);
        let id = [9_u8; 16];
        let store =
            CompletionStore::open(directory.clone(), Duration::from_secs(60), 4, 16).unwrap();
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
        let store = CompletionStore::open_with_active_limit(
            directory.clone(),
            Duration::from_secs(60),
            1024,
            16,
            1,
        )
        .unwrap();
        assert!(matches!(
            store.claim([1; 16]).await,
            CompletionClaim::Execute
        ));
        let waiting = match store.claim([2; 16]).await {
            CompletionClaim::WaitCapacity(notify) => notify,
            other => panic!("expected capacity wait, got {other:?}"),
        };
        store.abandon([1; 16]).await;
        tokio::time::timeout(Duration::from_millis(50), waiting.notified())
            .await
            .expect("stored capacity wake permit");
        assert!(matches!(
            store.claim([2; 16]).await,
            CompletionClaim::Execute
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn duplicate_waiter_cannot_miss_completion_wakeup() {
        let directory = temporary_directory("duplicate-wakeup");
        let _ = fs::remove_dir_all(&directory);
        let id = [3_u8; 16];
        let store =
            CompletionStore::open(directory.clone(), Duration::from_secs(60), 1024, 16).unwrap();
        assert!(matches!(store.claim(id).await, CompletionClaim::Execute));
        let waiting = match store.claim(id).await {
            CompletionClaim::WaitDuplicate(notify) => notify,
            other => panic!("expected duplicate wait, got {other:?}"),
        };
        store
            .record(id, Some(Bytes::from_static(b"done")))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(50), waiting.notified())
            .await
            .expect("stored duplicate wake permit");
        assert!(matches!(store.claim(id).await, CompletionClaim::Replay(_)));
        fs::remove_dir_all(directory).unwrap();
    }
}
