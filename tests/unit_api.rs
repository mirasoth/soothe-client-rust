//! Unit tests: protocol, public API surface, stream terminals.

use soothe_client::appkit::{input_message_for_loop, InputOpts};
use soothe_client::events::{
    EVENT_DEEP_RESEARCH_STARTED, EVENT_EXPLORER_COMPLETED, EVENT_EXPLORER_STARTED,
};
use soothe_client::intent_hints::{validate_loop_input_intent_hint, TEXT_COMPLETION};
use soothe_client::protocol::{
    expand_wire_messages, new_connection_init, new_request, new_request_id, PROTO_VERSION,
};
use soothe_client::stream_terminal::{is_turn_end_custom_data, is_turn_progress_chunk, STREAM_END};
use soothe_client::{AsyncCommandClient, Client, CommandClient, VERSION};

#[test]
fn public_api_symbols_exist() {
    let _ = Client::new("ws://127.0.0.1:8765");
    let _ = AsyncCommandClient::new("ws://127.0.0.1:8765");
    let _ = CommandClient::new("ws://127.0.0.1:8765");
    assert!(!VERSION.is_empty());
    assert_eq!(PROTO_VERSION, "1");
}

#[test]
fn protocol_request_roundtrip_json() {
    let env = new_request("daemon_status", Default::default());
    let json = env.to_wire_json().unwrap();
    assert!(json.contains("\"type\":\"request\""));
    assert!(json.contains("daemon_status"));
    let id = new_request_id();
    assert_eq!(id.len(), 32);
}

#[test]
fn connection_init_client_name() {
    let env = new_connection_init();
    let params = env.params.unwrap();
    assert_eq!(
        params.get("client_name").and_then(|v| v.as_str()),
        Some("soothe-client-rust")
    );
}

#[test]
fn expand_batch() {
    let batch = serde_json::json!({
        "type": "event_batch",
        "events": [{"type":"a"},{"type":"b"}]
    });
    assert_eq!(expand_wire_messages(batch).len(), 2);
}

#[test]
fn stream_terminal_helpers() {
    assert!(is_turn_end_custom_data(
        &serde_json::json!({"type": STREAM_END})
    ));
    assert!(is_turn_progress_chunk("messages", &serde_json::json!({})));
    assert!(validate_loop_input_intent_hint(TEXT_COMPLETION).is_none());
    assert!(validate_loop_input_intent_hint("direct_llm").is_some());
}

#[test]
fn preferred_subagent_in_loop_input() {
    let msg = input_message_for_loop(
        "find auth",
        "loop-1",
        None,
        Some(&InputOpts {
            intent_hint: Some(TEXT_COMPLETION.into()),
            preferred_subagent: Some("explorer".into()),
            ..Default::default()
        }),
    );
    assert_eq!(
        msg.get("preferred_subagent").and_then(|v| v.as_str()),
        Some("explorer")
    );
    assert_eq!(
        msg.get("intent_hint").and_then(|v| v.as_str()),
        Some(TEXT_COMPLETION)
    );
}

#[test]
fn subagent_event_constants() {
    assert_eq!(EVENT_EXPLORER_STARTED, "soothe.subagent.explorer.started");
    assert_eq!(
        EVENT_EXPLORER_COMPLETED,
        "soothe.subagent.explorer.completed"
    );
    assert_eq!(
        EVENT_DEEP_RESEARCH_STARTED,
        "soothe.subagent.deep_research.started"
    );
}
