use bytes::Bytes;
use veilid_http_core::RetryPolicy;
use veilid_http_engine::{InboundBody, OutboundBody};
use veilid_http_stream::{CompressionMode, DecodedFrame, StreamDirection, decode};

#[test]
fn end_waits_for_missing_data_and_duplicates_deliver_once() {
    let transaction = [4; 16];
    let mut sender = OutboundBody::new(
        transaction,
        StreamDirection::Response,
        CompressionMode::None,
        0,
        1024,
        128,
        8,
        64 * 1024,
    )
    .unwrap();
    sender.push(b"alpha", true).unwrap();
    sender.push(b"beta", true).unwrap();
    sender.finish_input().unwrap();
    let mut frames = sender
        .take_sendable(
            0,
            RetryPolicy {
                initial_ms: 10,
                maximum_ms: 100,
            },
        )
        .into_iter()
        .map(|frame| frame.encoded)
        .collect::<Vec<_>>();
    assert_eq!(frames.len(), 3);

    let end = frames.pop().unwrap();
    let first = frames.remove(0);
    let second = frames.remove(0);
    let mut receiver = InboundBody::new(
        transaction,
        StreamDirection::Response,
        CompressionMode::None,
        8,
        16 * 1024,
        1024,
    )
    .unwrap();
    assert!(!receiver.receive(end).unwrap().completed);
    let first_output = receiver.receive(first.clone()).unwrap();
    assert_eq!(
        first_output.logical_chunks,
        vec![Bytes::from_static(b"alpha")]
    );
    assert!(receiver.receive(first).unwrap().logical_chunks.is_empty());
    let second_output = receiver.receive(second).unwrap();
    assert_eq!(
        second_output.logical_chunks,
        vec![Bytes::from_static(b"beta")]
    );
    assert!(second_output.completed);
}

#[test]
fn dropped_frame_is_selectively_retried_and_large_stream_stays_bounded() {
    let transaction = [9; 16];
    let input = (0..120_000_u32)
        .flat_map(|value| value.wrapping_mul(2_654_435_761).to_le_bytes())
        .collect::<Vec<_>>();
    let mut sender = OutboundBody::new(
        transaction,
        StreamDirection::Request,
        CompressionMode::Zstd,
        3,
        2048,
        256,
        8,
        1024 * 1024,
    )
    .unwrap();
    let input_chunk = sender.max_input_chunk();
    for chunk in input.chunks(input_chunk) {
        sender.push(chunk, false).unwrap();
    }
    sender.finish_input().unwrap();
    let mut receiver = InboundBody::new(
        transaction,
        StreamDirection::Request,
        CompressionMode::Zstd,
        8,
        64 * 1024,
        u64::try_from(input.len()).unwrap(),
    )
    .unwrap();
    let policy = RetryPolicy {
        initial_ms: 10,
        maximum_ms: 100,
    };
    let mut now = 0;
    let mut dropped_one = false;
    let mut output = Vec::new();

    for _ in 0..20_000 {
        let mut sent = sender.take_sendable(now, policy);
        sent.reverse();
        for retained in sent {
            let sequence = match decode(retained.encoded.clone()).unwrap() {
                DecodedFrame::Data { sequence, .. } | DecodedFrame::End { sequence, .. } => {
                    sequence
                }
                _ => unreachable!(),
            };
            if sequence == 1 && !dropped_one {
                dropped_one = true;
                continue;
            }
            let received = receiver.receive(retained.encoded).unwrap();
            for chunk in received.logical_chunks {
                output.extend_from_slice(&chunk);
            }
            sender.acknowledge(received.ack).unwrap();
        }
        assert!(sender.in_flight_frames() <= 8);
        if sender.is_complete() && receiver.is_complete() {
            break;
        }
        now += 25;
    }

    assert!(dropped_one);
    assert!(sender.is_complete());
    assert!(receiver.is_complete());
    assert_eq!(output, input);
}

#[test]
fn peer_zero_window_stops_new_frames_and_reopens_cleanly() {
    let mut sender = OutboundBody::new(
        [1; 16],
        StreamDirection::Response,
        CompressionMode::None,
        0,
        1024,
        128,
        4,
        16 * 1024,
    )
    .unwrap();
    sender.set_peer_window(0).unwrap();
    sender.push(b"hello", true).unwrap();
    sender.finish_input().unwrap();
    assert_eq!(sender.in_flight_frames(), 0);
    assert!(sender.pending_bytes() > 0);
    sender.set_peer_window(4).unwrap();
    assert!(sender.in_flight_frames() > 0);
}
