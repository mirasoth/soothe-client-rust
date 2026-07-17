//! Unit tests: protocol, public API surface, stream terminals.

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
