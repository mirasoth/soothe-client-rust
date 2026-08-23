//! Offline appkit unit tests (Go `appkit/*_test.go` coverage parity).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map};
use soothe_client::appkit::{
    compact_attachments, default_thinking_step_events, extract_thinking_step,
    should_drop_stream_chunk_early, CancelFn, ChatEventTerminal, ClassifierConfig, EventClassifier,
    InMemoryLoopSessionStore, LoopSessionEntry, LoopSessionStore, QueryGate, SendCancelFn,
    SseBroadcaster, SseEvent, TurnBoundary, TURN_END_IDLE, TURN_END_STREAM_END,
};
#[cfg(feature = "image")]
use soothe_client::appkit::{compact_image_attachment, CompactImageOptions};
use soothe_client::intent_hints::DEFAULT_DELIVERABLE_PHASES;
use soothe_client::stream_terminal::STREAM_END;

#[tokio::test]
async fn query_gate_single_flight() {
    let gate = QueryGate::new();
    let cancelled = Arc::new(AtomicBool::new(false));
    let c1 = cancelled.clone();
    gate.acquire(
        "s1",
        Arc::new(move || c1.store(true, Ordering::SeqCst)),
        None,
    )
    .expect("first acquire");
    assert!(gate.is_active("s1"));
    assert!(gate.acquire("s1", Arc::new(|| {}), None).is_err());
    gate.release("s1");
    assert!(!gate.is_active("s1"));
    gate.acquire("s1", Arc::new(|| {}), None)
        .expect("re-acquire");
    gate.release("s1");
}

#[tokio::test]
async fn query_gate_cancel_ordering() {
    let gate = QueryGate::new();
    let order = Arc::new(AtomicUsize::new(0));
    let daemon_step = Arc::new(AtomicUsize::new(0));
    let local_step = Arc::new(AtomicUsize::new(0));

    let order_d = order.clone();
    let daemon_step_c = daemon_step.clone();
    let send_cancel: SendCancelFn = Arc::new(move || {
        let order_d = order_d.clone();
        let daemon_step_c = daemon_step_c.clone();
        Box::pin(async move {
            daemon_step_c.store(order_d.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
            Ok(())
        })
    });

    let order_l = order.clone();
    let local_step_c = local_step.clone();
    let cancel: CancelFn = Arc::new(move || {
        local_step_c.store(order_l.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
    });

    gate.acquire("s1", cancel, Some(send_cancel)).unwrap();
    gate.cancel("s1").await.unwrap();
    assert_eq!(daemon_step.load(Ordering::SeqCst), 1);
    assert_eq!(local_step.load(Ordering::SeqCst), 2);
    assert!(!gate.is_active("s1"));
}

#[tokio::test]
async fn sse_broadcaster_subscribe_broadcast_close() {
    let b = SseBroadcaster::new();
    let (id, mut rx) = b.subscribe("chat-1");
    b.broadcast(
        "chat-1",
        SseEvent {
            event_type: "delta".into(),
            data: json!("hi"),
        },
    );
    let ev = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("event");
    assert_eq!(ev.event_type, "delta");
    assert_eq!(ev.data, json!("hi"));
    b.unsubscribe("chat-1", &id);
    b.broadcast(
        "chat-1",
        SseEvent {
            event_type: "delta".into(),
            data: json!("gone"),
        },
    );
    assert!(rx.try_recv().is_err());
    let (_id2, mut rx2) = b.subscribe("chat-2");
    b.close_all();
    assert!(rx2.recv().await.is_none());
}

#[tokio::test]
async fn sse_broadcaster_drop_on_full() {
    let b = SseBroadcaster::new();
    let (_id, mut rx) = b.subscribe("busy");
    // Fill the 100-cap buffer without consuming.
    for i in 0..120 {
        b.broadcast(
            "busy",
            SseEvent {
                event_type: "delta".into(),
                data: json!(i),
            },
        );
    }
    // First events should still be readable; overflow was dropped (non-blocking).
    let first = rx.recv().await.expect("first");
    assert_eq!(first.event_type, "delta");
    let mut n = 1usize;
    while rx.try_recv().is_ok() {
        n += 1;
    }
    assert!(n <= 100, "buffer capacity 100, got {n}");
}

#[test]
fn chunk_filter_updates_and_messages() {
    assert!(should_drop_stream_chunk_early(&[], "updates", &json!({})));
    assert!(!should_drop_stream_chunk_early(
        &[],
        "updates",
        &json!({"__interrupt__": true})
    ));
    assert!(!should_drop_stream_chunk_early(&[], "custom", &json!({})));
    // Non-actionable empty messages tuple drops.
    assert!(should_drop_stream_chunk_early(
        &[],
        "messages",
        &json!([null, {}])
    ));
}

#[test]
fn thinking_step_extraction() {
    let allow = default_thinking_step_events();
    let mut data = Map::new();
    data.insert("step_id".into(), json!("s1"));
    data.insert("description".into(), json!("plan next"));
    let line = extract_thinking_step(Some(&allow), "soothe.cognition.plan.step.started", &data)
        .expect("line");
    assert!(line.contains("s1"));
    assert!(extract_thinking_step(Some(&allow), "soothe.unknown", &data).is_none());
}

#[test]
fn classifier_deliverable_and_streaming() {
    let cl = EventClassifier::with_defaults();
    let goal = json!({
        "type": "event",
        "mode": "messages",
        "data": [{"type":"AIMessage","phase":"goal_completion","content":"12345678 done"}]
    });
    let r = cl.classify(&goal, "");
    assert_eq!(r.terminal, ChatEventTerminal::DeliverableComplete);

    let stream = json!({
        "type": "event",
        "mode": "messages",
        "data": [{"type":"AIMessageChunk","content":"partial"}]
    });
    let r2 = cl.classify(&stream, "");
    assert_eq!(r2.terminal, ChatEventTerminal::Continue);
    assert!(!r2.content.is_empty());
}

#[test]
fn classifier_phase_not_in_config() {
    let mut phases: std::collections::HashSet<String> = DEFAULT_DELIVERABLE_PHASES
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    phases.remove("goal_completion");
    let cl = EventClassifier::new(ClassifierConfig {
        deliverable_phases: phases,
        ..Default::default()
    })
    .unwrap();
    let goal = json!({
        "type": "event",
        "mode": "messages",
        "data": [{"type":"AIMessage","phase":"goal_completion","content":"12345678 done"}]
    });
    let r = cl.classify(&goal, "");
    assert_ne!(r.terminal, ChatEventTerminal::DeliverableComplete);
}

#[test]
fn turn_boundary_stream_end_and_idle() {
    let mut b = TurnBoundary::default();
    let end = json!({"type": STREAM_END, "scope": "turn", "turn_id": "L:1"});
    assert!(b.feed_event("custom", &end).is_none());
    b.feed_status_turn("running", Some("L:1"));
    b.feed_event_turn("messages", &json!([{"content":"x"}]), Some("L:1"));
    assert_eq!(
        b.feed_event_turn("custom", &end, Some("L:1")),
        Some(TURN_END_STREAM_END)
    );

    let mut b2 = TurnBoundary::default();
    b2.feed_status_turn("running", Some("L:1"));
    b2.feed_event_turn("messages", &json!([{"content":"y"}]), Some("L:1"));
    assert_eq!(
        b2.feed_status_turn("idle", Some("L:1")),
        Some(TURN_END_IDLE)
    );
}

#[test]
fn loop_session_store_roundtrip() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let store = InMemoryLoopSessionStore::new();
        store
            .create_session(LoopSessionEntry {
                session_id: "s1".into(),
                loop_id: Some("loop-1".into()),
                workspace_id: "ws".into(),
                user_id: "u".into(),
                ..Default::default()
            })
            .await;
        assert_eq!(
            store.get_loop_id_for_session("s1").await.as_deref(),
            Some("loop-1")
        );
        store
            .append_message("s1", json!({"role":"user","content":"hi"}))
            .await;
        let rec = store.get_session("s1").await.unwrap();
        assert_eq!(rec.messages.len(), 1);
    });
}

#[test]
fn compact_attachments_passthrough_without_image_bytes() {
    let mut att = Map::new();
    att.insert("mime_type".into(), json!("text/plain"));
    att.insert("data".into(), json!("aGVsbG8="));
    let out = compact_attachments(&[att.clone()], None);
    assert_eq!(
        out[0].get("mime_type").and_then(|v| v.as_str()),
        Some("text/plain")
    );
}

#[cfg(feature = "image")]
#[test]
fn compact_image_downscales_large_png() {
    use base64::Engine;
    use image::codecs::png::PngEncoder;
    use image::{ExtendedColorType, ImageBuffer, ImageEncoder, Rgba};

    let img: ImageBuffer<Rgba<u8>, Vec<u8>> = ImageBuffer::from_fn(1200, 800, |x, y| {
        Rgba([(x % 255) as u8, (y % 255) as u8, 0, 255])
    });
    let mut png = Vec::new();
    PngEncoder::new(&mut png)
        .write_image(img.as_raw(), 1200, 800, ExtendedColorType::Rgba8)
        .unwrap();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&png);
    let (mime, out) = compact_image_attachment(
        "image/png",
        &b64,
        Some(&CompactImageOptions {
            max_dim: 256,
            jpeg_quality: 85,
        }),
    );
    assert_eq!(mime, "image/png");
    assert!(
        out.len() < b64.len(),
        "expected smaller payload after downscale"
    );
}

#[cfg(feature = "image")]
#[test]
fn compact_image_passthrough_small() {
    use base64::Engine;
    use image::codecs::png::PngEncoder;
    use image::{ExtendedColorType, ImageBuffer, ImageEncoder, Rgba};

    let img: ImageBuffer<Rgba<u8>, Vec<u8>> = ImageBuffer::from_pixel(10, 10, Rgba([1, 2, 3, 255]));
    let mut png = Vec::new();
    PngEncoder::new(&mut png)
        .write_image(img.as_raw(), 10, 10, ExtendedColorType::Rgba8)
        .unwrap();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&png);
    let (mime, out) = compact_image_attachment("image/png", &b64, None);
    assert_eq!(mime, "image/png");
    assert_eq!(out, b64);
}

#[test]
fn inbound_priority_exports() {
    use soothe_client::{
        inbound_frame_drop_priority, DROP_PRIORITY_CRITICAL, DROP_PRIORITY_HIGH,
        DROP_PRIORITY_NORMAL,
    };
    assert_eq!(inbound_frame_drop_priority(None), DROP_PRIORITY_CRITICAL);
    let mut m = Map::new();
    m.insert("type".into(), json!("event_batch"));
    assert_eq!(inbound_frame_drop_priority(Some(&m)), DROP_PRIORITY_HIGH);
    m.insert("type".into(), json!("event"));
    m.insert("mode".into(), json!("updates"));
    assert_eq!(inbound_frame_drop_priority(Some(&m)), DROP_PRIORITY_NORMAL);
}
