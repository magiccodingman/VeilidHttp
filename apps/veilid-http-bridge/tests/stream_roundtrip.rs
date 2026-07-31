//! End-to-end streamed bridge round-trip and duplicate-suppression tests.

use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::HashMap,
    fs,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpListener,
    sync::{Mutex, mpsc},
};
use veilid_http_bridge::{
    completion::{CompletionLookup, CompletionStore},
    streaming::{StreamingBridge, StreamingConfig},
};
use veilid_http_engine::InboundBody;
use veilid_http_http::{HeaderField, RequestHead};
use veilid_http_stream::{
    CompressionMode, DecodedFrame, RequestOpen, StreamDirection, decode, encode_ack,
    encode_request_open,
};
use veilid_http_transport::{RouteTarget, TransportError, TransportEvent, VeilidTransport};
use veilid_http_wire::{Frame, FrameBundle};

#[derive(Debug)]
struct MockTransport {
    replies: mpsc::UnboundedSender<(String, Bytes)>,
    messages: mpsc::UnboundedSender<(RouteTarget, Bytes)>,
    imports: Mutex<HashMap<Vec<u8>, RouteTarget>>,
}

#[async_trait]
impl VeilidTransport for MockTransport {
    async fn import_route(&self, route_blob: Bytes) -> Result<RouteTarget, TransportError> {
        let key = route_blob.to_vec();
        let mut imports = self.imports.lock().await;
        if let Some(target) = imports.get(&key) {
            return Ok(target.clone());
        }
        let target = RouteTarget(format!("imported-{}", imports.len()));
        imports.insert(key, target.clone());
        Ok(target)
    }

    async fn allocate_route(&self) -> Result<(RouteTarget, Bytes), TransportError> {
        Err(TransportError::Fatal("not used by bridge test".to_owned()))
    }

    async fn release_route(&self, _target: &RouteTarget) -> Result<(), TransportError> {
        Ok(())
    }

    async fn app_call(
        &self,
        _target: &RouteTarget,
        _payload: Bytes,
    ) -> Result<Bytes, TransportError> {
        Err(TransportError::Fatal("not used by bridge test".to_owned()))
    }

    async fn app_message(
        &self,
        target: &RouteTarget,
        payload: Bytes,
    ) -> Result<(), TransportError> {
        self.messages
            .send((target.clone(), payload))
            .map_err(|_| TransportError::Shutdown)
    }

    async fn app_call_reply(&self, call_id: &str, payload: Bytes) -> Result<(), TransportError> {
        self.replies
            .send((call_id.to_owned(), payload))
            .map_err(|_| TransportError::Shutdown)
    }

    async fn next_event(&self) -> Result<TransportEvent, TransportError> {
        Ok(TransportEvent::Shutdown)
    }
}

fn split_frames(payload: Bytes) -> Vec<Bytes> {
    FrameBundle::decode(payload.clone()).map_or_else(
        |_| {
            Frame::decode(payload.clone()).expect("single VHTTP frame");
            vec![payload]
        },
        |bundle| bundle.frames,
    )
}

async fn start_http_fixture() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let task_hits = Arc::clone(&hits);
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let hits = Arc::clone(&task_hits);
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = socket.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                hits.fetch_add(1, Ordering::SeqCst);
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 17\r\nConnection: close\r\n\r\nstreamed-response",
                    )
                    .await
                    .unwrap();
            });
        }
    });
    (format!("http://{address}"), hits, task)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn streamed_get_forwards_once_and_rejects_completed_duplicate() {
    let (upstream, hits, server_task) = start_http_fixture().await;
    let root = std::env::temp_dir().join(format!(
        "veilid-http-stream-roundtrip-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    let completion = CompletionStore::open(
        root.join("completed"),
        Duration::from_secs(60),
        1024 * 1024,
        128,
    )
    .unwrap();
    let (reply_sender, mut replies) = mpsc::unbounded_channel();
    let (message_sender, mut messages) = mpsc::unbounded_channel();
    let transport = Arc::new(MockTransport {
        replies: reply_sender,
        messages: message_sender,
        imports: Mutex::new(HashMap::new()),
    });
    let transport_trait: Arc<dyn VeilidTransport> = transport;
    let bridge = StreamingBridge::new(
        transport_trait,
        reqwest::Client::new(),
        Arc::clone(&completion),
        StreamingConfig {
            upstream_url: upstream,
            route_header: "X-Veilid-Route-Fingerprint".to_owned(),
            frame_bytes: 1024,
            window_frames: 8,
            max_pending_bytes: 64 * 1024,
            max_out_of_order_bytes: 64 * 1024,
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 1024 * 1024,
            overall_timeout: Duration::from_secs(5),
        },
    );

    let transaction_id = [4_u8; 16];
    let opening = encode_request_open(
        transaction_id,
        &RequestOpen {
            head: RequestHead {
                method: "GET".to_owned(),
                path_and_query: "/fixture".to_owned(),
                headers: vec![HeaderField {
                    name: "accept".to_owned(),
                    value: "text/plain".to_owned(),
                }],
            },
            return_route_blob: b"private-client-return-route".to_vec(),
            request_compression: CompressionMode::Zstd,
            request_body: false,
            response_receive_window: 8,
        },
        Bytes::new(),
    )
    .unwrap();

    bridge
        .handle_open("call-one", "site-fingerprint".to_owned(), opening.clone())
        .await
        .unwrap();
    let (call_id, accepted) = tokio::time::timeout(Duration::from_secs(2), replies.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(call_id, "call-one");
    assert!(matches!(
        decode(accepted).unwrap(),
        DecodedFrame::RequestAccepted { transaction_id: id, .. } if id == transaction_id
    ));

    let mut receiver = None;
    let mut response_body = Vec::new();
    let mut response_completed = false;
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    while !response_completed {
        tokio::select! {
            message = messages.recv() => {
                let (_, payload) = message.expect("bridge response message");
                for frame in split_frames(payload) {
                    match decode(frame.clone()).unwrap() {
                        DecodedFrame::ResponseOpen { transaction_id: id, value, .. } => {
                            assert_eq!(id, transaction_id);
                            assert_eq!(value.head.status, 200);
                            receiver = Some(InboundBody::new(
                                transaction_id,
                                StreamDirection::Response,
                                value.response_compression,
                                8,
                                64 * 1024,
                                1024 * 1024,
                            ).unwrap());
                        }
                        DecodedFrame::Data { transaction_id: id, direction: StreamDirection::Response, .. }
                        | DecodedFrame::End { transaction_id: id, value: veilid_http_stream::StreamEnd { direction: StreamDirection::Response, .. }, .. } => {
                            assert_eq!(id, transaction_id);
                            let output = receiver.as_mut().expect("ResponseOpen before data").receive(frame).unwrap();
                            for chunk in output.logical_chunks {
                                response_body.extend_from_slice(&chunk);
                            }
                            bridge.handle_message(encode_ack(transaction_id, output.ack).unwrap()).await.unwrap();
                            response_completed = output.completed;
                        }
                        other => panic!("unexpected server frame: {other:?}"),
                    }
                }
            }
            () = &mut deadline => panic!("streamed response timed out"),
        }
    }
    assert_eq!(response_body, b"streamed-response");

    let completion_deadline = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(completion_deadline);
    loop {
        if matches!(
            completion.lookup(transaction_id).await,
            Some(CompletionLookup::Tombstone)
        ) {
            break;
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
            () = &mut completion_deadline => panic!("completion tombstone was not recorded"),
        }
    }

    bridge
        .handle_open("call-two", "site-fingerprint".to_owned(), opening)
        .await
        .unwrap();
    let (call_id, duplicate_reply) = tokio::time::timeout(Duration::from_secs(2), replies.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(call_id, "call-two");
    assert!(matches!(
        decode(duplicate_reply).unwrap(),
        DecodedFrame::Error { transaction_id: id, .. } if id == transaction_id
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    server_task.abort();
    fs::remove_dir_all(root).unwrap();
}
