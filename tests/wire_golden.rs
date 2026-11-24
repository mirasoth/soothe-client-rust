//! Client-side wire golden contract test.
//!
//! The fixture (\`tests/fixtures/wire_golden.json\`) is byte-identical to the
//! monorepo copy (\`tests/wire_fixtures/wire_golden.json\`). Both sides pin its
//! sha256, so a wire-format change forces a coordinated regen + commit.

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use soothe_client::card_projection::{parse_card_custom_payload, CardProjection};
use soothe_client::events::EVENT_STREAM_TOOL_CALL_UPDATE;
use soothe_client::protocol::{decode_message, decode_message_typed, TypedMessage};
use soothe_client::stream_terminal::is_stream_terminal_data;

const FIXTURE_SHA256: &str = "38904a5672882b345a3f9786566893220e1e37da51336df22301ade30b5a6cef";

const ENVELOPE_CASES: &[&str] = &["connection_init", "response", "error"];
const MESSAGE_CASES: &[&str] = &[
    "ai_message_with_tool_calls",
    "ai_chunk",
    "human_message",
    "tool_message",
];

fn fixture_text() -> String {
    std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/wire_golden.json"
    ))
    .expect("read fixture")
}

fn fixture() -> Value {
    serde_json::from_str(&fixture_text()).expect("parse fixture")
}

#[test]
fn fixture_is_pinned() {
    let bytes = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/wire_golden.json"
    ))
    .expect("read fixture bytes");
    let digest = format!("{:x}", Sha256::digest(&bytes));
    assert_eq!(
        digest, FIXTURE_SHA256,
        "wire golden fixture changed — regenerate in the soothe monorepo, copy here, and update FIXTURE_SHA256 in both host and client contract tests"
    );
}

#[test]
fn response_wire_text_decodes_to_response_case() {
    let doc = fixture();
    let text = doc["response_wire_text"].as_str().unwrap();
    assert_eq!(decode_message(text).unwrap(), doc["cases"]["response"]);
}

#[test]
fn envelope_cases_round_trip() {
    let doc = fixture();
    for case in ENVELOPE_CASES {
        let envelope = &doc["cases"][case];
        let text = serde_json::to_string(envelope).unwrap();
        let decoded = decode_message(&text).unwrap();
        assert_eq!(&decoded, envelope, "decode mismatch for {case}");
        let retext = serde_json::to_string(&decoded).unwrap();
        assert_eq!(
            decode_message(&retext).unwrap(),
            *envelope,
            "re-encode mismatch for {case}"
        );
    }
}

#[test]
fn batch_round_trips() {
    let doc = fixture();
    let batch = &doc["cases"]["batch"];
    let text = serde_json::to_string(batch).unwrap();
    assert_eq!(decode_message(&text).unwrap(), *batch);
}

#[test]
fn message_cases_decode_without_nested_error_semantics() {
    // Legacy flat payload shapes decode as opaque values; the codec must not
    // force them into the envelope's nested-error model.
    let doc = fixture();
    for case in MESSAGE_CASES {
        let wire = &doc["cases"][case];
        let text = serde_json::to_string(wire).unwrap();
        let decoded = decode_message(&text).unwrap();
        assert_eq!(&decoded, wire, "decode mismatch for {case}");
        let typed = decode_message_typed(&text).unwrap();
        assert!(
            matches!(typed, TypedMessage::Value(_)),
            "{case} should stay an opaque Value"
        );
    }
}

#[test]
fn ai_tool_calls_survive_decode() {
    let doc = fixture();
    let wire = &doc["cases"]["ai_message_with_tool_calls"];
    assert_eq!(
        wire["tool_calls"][0],
        json!({
            "name": "read_file",
            "args": {"path": "docs/README.md", "offset": 0},
            "id": "call_golden_1",
            "type": "tool_call",
        })
    );
}

#[test]
fn stream_tool_update_case_matches_live_constant() {
    let doc = fixture();
    let wire = &doc["cases"]["stream_tool_update"];
    assert_eq!(
        wire["type"].as_str().unwrap(),
        EVENT_STREAM_TOOL_CALL_UPDATE
    );
    assert!(is_stream_terminal_data(&json!([
        {"stream_terminal": true, "chunk_position": "last"},
        {}
    ])));
}

#[test]
fn card_frame_passthrough_via_projection() {
    let doc = fixture();
    let frame = doc["cases"]["card_frame"].clone();
    let card_id = frame["id"].as_str().unwrap().to_string();

    // Custom-mode created frame carries the golden card verbatim.
    let custom = json!({"type": "soothe.card.created", "data": frame});
    let parsed = parse_card_custom_payload(&custom).expect("card frame");
    assert_eq!(parsed.wire_type, "soothe.card.created");
    assert_eq!(Value::Object(parsed.card.unwrap()), frame);

    // Projection stores it verbatim and returns it in order.
    let mut projection = CardProjection::new();
    assert!(projection.apply(&custom));
    assert!(!projection.replaying());
    assert_eq!(
        Value::Object(projection.get(&card_id).unwrap().clone()),
        frame
    );
    assert_eq!(projection.snapshot().len(), 1);
}
