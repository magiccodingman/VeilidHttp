//! Native `veilid-core` implementation of the VeilidHttp transport contract.

use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
};
use tokio::sync::{Mutex, mpsc};
use veilid_core::{
    OperationId, RouteId, RoutingContext, Target, UpdateCallback, VeilidAPI, VeilidAPIError,
    VeilidConfig, VeilidUpdate, api_startup_json,
};
use veilid_http_transport::{RouteTarget, TransportError, TransportEvent, VeilidTransport};

/// Configuration for an embedded native Veilid node.
#[derive(Debug, Clone)]
pub struct NativeTransportConfig {
    /// Stable Veilid program name. Changing this may orphan protected state.
    pub program_name: String,
    /// Organization portion of the platform bundle identifier.
    pub organization: String,
    /// Qualifier portion of the platform bundle identifier.
    pub qualifier: String,
    /// Directory containing Veilid table, block, route-spec, and protected stores.
    pub storage_directory: std::path::PathBuf,
    /// Optional directory containing extra Veilid configuration files.
    pub config_directory: Option<std::path::PathBuf>,
}

impl NativeTransportConfig {
    /// Build the default VeilidHttp client configuration.
    #[must_use]
    pub fn client(storage_directory: impl Into<std::path::PathBuf>) -> Self {
        Self {
            program_name: "veilid_http_client".to_owned(),
            organization: "veilidhttp".to_owned(),
            qualifier: "org".to_owned(),
            storage_directory: storage_directory.into(),
            config_directory: None,
        }
    }
}

/// Embedded native Veilid transport.
pub struct NativeVeilidTransport {
    api: VeilidAPI,
    routing: RoutingContext,
    events: Mutex<mpsc::UnboundedReceiver<TransportEvent>>,
    pending_calls: Arc<StdMutex<HashMap<String, OperationId>>>,
}

impl std::fmt::Debug for NativeVeilidTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeVeilidTransport")
            .finish_non_exhaustive()
    }
}

impl NativeVeilidTransport {
    /// Start and attach an embedded Veilid node using persistent storage.
    ///
    /// # Errors
    ///
    /// Returns a transport error when configuration serialization, Veilid startup,
    /// attachment, or routing-context creation fails.
    pub async fn start(config: NativeTransportConfig) -> Result<Self, TransportError> {
        std::fs::create_dir_all(&config.storage_directory)
            .map_err(|error| TransportError::Fatal(error.to_string()))?;

        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        let pending_calls = Arc::new(StdMutex::new(HashMap::new()));
        let callback_calls = Arc::clone(&pending_calls);
        let update_callback: UpdateCallback = Arc::new(move |update| {
            map_update(update, &event_sender, &callback_calls);
        });

        let storage = path_to_utf8(&config.storage_directory)?;
        let config_directory = config
            .config_directory
            .as_deref()
            .map(path_to_utf8)
            .transpose()?;
        let veilid_config = VeilidConfig::new(
            &config.program_name,
            &config.organization,
            &config.qualifier,
            Some(storage),
            config_directory,
        );
        let config_json = serde_json::to_string(&veilid_config)
            .map_err(|error| TransportError::Fatal(error.to_string()))?;
        let api = api_startup_json(update_callback, config_json)
            .await
            .map_err(classify_error)?;
        api.attach().await.map_err(classify_error)?;
        let routing = api.routing_context().map_err(classify_error)?;

        Ok(Self {
            api,
            routing,
            events: Mutex::new(event_receiver),
            pending_calls,
        })
    }

    /// Shut down the embedded Veilid node.
    pub async fn shutdown(self) {
        self.api.shutdown().await;
    }
}

fn path_to_utf8(path: &Path) -> Result<&str, TransportError> {
    path.to_str().ok_or_else(|| {
        TransportError::Fatal(format!(
            "Veilid storage path is not valid UTF-8: {}",
            path.display()
        ))
    })
}

fn map_update(
    update: VeilidUpdate,
    sender: &mpsc::UnboundedSender<TransportEvent>,
    pending_calls: &StdMutex<HashMap<String, OperationId>>,
) {
    match update {
        VeilidUpdate::AppMessage(message) => {
            let route = message
                .route_id()
                .map(|route| RouteTarget(route.to_string()));
            let _ = sender.send(TransportEvent::AppMessage {
                route,
                payload: Bytes::copy_from_slice(message.message()),
            });
        }
        VeilidUpdate::AppCall(call) => {
            let call_id = call.id().to_string();
            if let Ok(mut calls) = pending_calls.lock() {
                calls.insert(call_id.clone(), call.id());
            } else {
                tracing::error!("native Veilid pending-call registry was poisoned");
                return;
            }
            let route = call.route_id().map(|route| RouteTarget(route.to_string()));
            let _ = sender.send(TransportEvent::AppCall {
                call_id,
                route,
                payload: Bytes::copy_from_slice(call.message()),
            });
        }
        VeilidUpdate::RouteChange(change) => {
            for route in change.dead_routes.iter().chain(&change.dead_remote_routes) {
                let _ = sender.send(TransportEvent::RouteChanged {
                    route: RouteTarget(route.to_string()),
                    dead: true,
                });
            }
        }
        VeilidUpdate::Shutdown => {
            let _ = sender.send(TransportEvent::Shutdown);
        }
        VeilidUpdate::Log(_)
        | VeilidUpdate::Attachment(_)
        | VeilidUpdate::Network(_)
        | VeilidUpdate::Config(_)
        | VeilidUpdate::ValueChange(_) => {}
    }
}

fn parse_route(target: &RouteTarget) -> Result<RouteId, TransportError> {
    RouteId::try_from(target.0.as_str())
        .map_err(|error| TransportError::InvalidTarget(error.to_string()))
}

fn classify_error(error: VeilidAPIError) -> TransportError {
    let message = error.to_string();
    let normalized = message.to_ascii_lowercase();
    if normalized.contains("timeout") {
        TransportError::Timeout
    } else if normalized.contains("no connection")
        || normalized.contains("try again")
        || normalized.contains("temporarily")
        || normalized.contains("not attached")
    {
        TransportError::Retryable(message)
    } else {
        TransportError::Fatal(message)
    }
}

#[async_trait]
impl VeilidTransport for NativeVeilidTransport {
    async fn import_route(&self, route_blob: Bytes) -> Result<RouteTarget, TransportError> {
        let route = self
            .api
            .import_remote_private_route(route_blob.to_vec())
            .map_err(classify_error)?;
        Ok(RouteTarget(route.to_string()))
    }

    async fn allocate_route(&self) -> Result<(RouteTarget, Bytes), TransportError> {
        let route = self.api.new_private_route().await.map_err(classify_error)?;
        Ok((
            RouteTarget(route.route_id.to_string()),
            Bytes::from(route.blob),
        ))
    }

    async fn release_route(&self, target: &RouteTarget) -> Result<(), TransportError> {
        self.api
            .release_private_route(parse_route(target)?)
            .map_err(classify_error)
    }

    async fn app_call(
        &self,
        target: &RouteTarget,
        payload: Bytes,
    ) -> Result<Bytes, TransportError> {
        self.routing
            .app_call(Target::PrivateRoute(parse_route(target)?), payload.to_vec())
            .await
            .map(Bytes::from)
            .map_err(classify_error)
    }

    async fn app_message(
        &self,
        target: &RouteTarget,
        payload: Bytes,
    ) -> Result<(), TransportError> {
        self.routing
            .app_message(Target::PrivateRoute(parse_route(target)?), payload.to_vec())
            .await
            .map_err(classify_error)
    }

    async fn app_call_reply(&self, call_id: &str, payload: Bytes) -> Result<(), TransportError> {
        let operation_id = self
            .pending_calls
            .lock()
            .map_err(|_| TransportError::Fatal("pending-call registry was poisoned".to_owned()))?
            .remove(call_id)
            .ok_or_else(|| {
                TransportError::InvalidTarget("unknown or already-replied AppCall".to_owned())
            })?;
        self.api
            .app_call_reply(operation_id, payload.to_vec())
            .await
            .map_err(classify_error)
    }

    async fn next_event(&self) -> Result<TransportEvent, TransportError> {
        self.events
            .lock()
            .await
            .recv()
            .await
            .ok_or(TransportError::Shutdown)
    }
}
