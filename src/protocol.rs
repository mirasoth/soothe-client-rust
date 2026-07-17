//! Protocol-1 wire envelope encode/decode.

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::VERSION;

/// Protocol version string.
pub const PROTO_VERSION: &str = "1";

/// Client version reported in `connection_init` (crate version).
pub const CLIENT_VERSION: &str = VERSION;

/// Default capabilities declared in the handshake.
pub const DEFAULT_CLIENT_CAPABILITIES: &[&str] = &["streaming", "batch", "heartbeat", "receipts"];

/// Message class values for the envelope `type` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    /// Client→server handshake.
    ConnectionInit,
    /// Server→client handshake ack.
    ConnectionAck,
    /// RPC request.
    Request,
    /// RPC success response.
    Response,
    /// Fire-and-forget notification.
    Notification,
    /// Start a subscription.
    Subscribe,
    /// Stream event.
    Next,
    /// Structured error.
    Error,
    /// Stream completion.
    Complete,
    /// Cancel subscription.
    Unsubscribe,
    /// Heartbeat ping.
    Ping,
    /// Heartbeat pong.
    Pong,
    /// Receipt confirmation.
    ReceiptResponse,
    /// Graceful disconnect.
    Disconnect,
    /// Status frame (often top-level).
    Status,
}

impl MessageType {
    /// Wire string.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConnectionInit => "connection_init",
            Self::ConnectionAck => "connection_ack",
            Self::Request => "request",
            Self::Response => "response",
            Self::Notification => "notification",
            Self::Subscribe => "subscribe",
            Self::Next => "next",
            Self::Error => "error",
            Self::Complete => "complete",
            Self::Unsubscribe => "unsubscribe",
            Self::Ping => "ping",
            Self::Pong => "pong",
            Self::ReceiptResponse => "receipt_response",
            Self::Disconnect => "disconnect",
            Self::Status => "status",
        }
    }
}

/// Structured error nested under envelope.error.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorObject {
    /// Numeric code.
    pub code: i64,
    /// Message.
    pub message: String,
    /// Optional data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// Unified `{proto, type, method, params, id}` envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Envelope {
    /// Protocol version.
    pub proto: String,
    /// Message class.
    #[serde(rename = "type")]
    pub msg_type: String,
    /// RPC / subscription method.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Structured params.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Map<String, Value>>,
    /// Correlation id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Success result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Map<String, Value>>,
    /// Structured error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorObject>,
    /// Stream payload (`next`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// Optional receipt id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<String>,
    /// Extra fields (status frames, etc.).
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Envelope {
    /// Compact JSON text (no spaces).
    pub fn to_wire_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Convert to a generic JSON object.
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// Generate a 32-hex request id (UUID without dashes).
pub fn new_request_id() -> String {
    Uuid::new_v4().simple().to_string()
}

/// Build a request envelope.
pub fn new_request(method: impl Into<String>, params: Map<String, Value>) -> Envelope {
    Envelope {
        proto: PROTO_VERSION.to_string(),
        msg_type: "request".into(),
        method: Some(method.into()),
        params: if params.is_empty() {
            None
        } else {
            Some(params)
        },
        id: Some(new_request_id()),
        result: None,
        error: None,
        payload: None,
        receipt: None,
        extra: Map::new(),
    }
}

/// Build a notification envelope (no id).
pub fn new_notification(method: impl Into<String>, params: Map<String, Value>) -> Envelope {
    Envelope {
        proto: PROTO_VERSION.to_string(),
        msg_type: "notification".into(),
        method: Some(method.into()),
        params: if params.is_empty() {
            None
        } else {
            Some(params)
        },
        id: None,
        result: None,
        error: None,
        payload: None,
        receipt: None,
        extra: Map::new(),
    }
}

/// Build a subscribe envelope.
pub fn new_subscribe(method: impl Into<String>, params: Map<String, Value>) -> Envelope {
    Envelope {
        proto: PROTO_VERSION.to_string(),
        msg_type: "subscribe".into(),
        method: Some(method.into()),
        params: if params.is_empty() {
            None
        } else {
            Some(params)
        },
        id: Some(new_request_id()),
        result: None,
        error: None,
        payload: None,
        receipt: None,
        extra: Map::new(),
    }
}

/// Build an unsubscribe envelope.
pub fn new_unsubscribe(id: impl Into<String>) -> Envelope {
    Envelope {
        proto: PROTO_VERSION.to_string(),
        msg_type: "unsubscribe".into(),
        method: None,
        params: None,
        id: Some(id.into()),
        result: None,
        error: None,
        payload: None,
        receipt: None,
        extra: Map::new(),
    }
}

/// Build connection_init.
pub fn new_connection_init() -> Envelope {
    let mut params = Map::new();
    params.insert("client_version".into(), json!(CLIENT_VERSION));
    params.insert("client_name".into(), json!("soothe-client-rust"));
    params.insert("accept_proto".into(), json!([PROTO_VERSION]));
    params.insert("capabilities".into(), json!(DEFAULT_CLIENT_CAPABILITIES));
    Envelope {
        proto: PROTO_VERSION.to_string(),
        msg_type: "connection_init".into(),
        method: None,
        params: Some(params),
        id: None,
        result: None,
        error: None,
        payload: None,
        receipt: None,
        extra: Map::new(),
    }
}

/// Build ping.
pub fn new_ping() -> Envelope {
    Envelope {
        proto: PROTO_VERSION.to_string(),
        msg_type: "ping".into(),
        method: None,
        params: None,
        id: None,
        result: None,
        error: None,
        payload: None,
        receipt: None,
        extra: Map::new(),
    }
}

/// Build pong.
pub fn new_pong() -> Envelope {
    Envelope {
        proto: PROTO_VERSION.to_string(),
        msg_type: "pong".into(),
        method: None,
        params: None,
        id: None,
        result: None,
        error: None,
        payload: None,
        receipt: None,
        extra: Map::new(),
    }
}

/// Build disconnect notification.
pub fn new_disconnect() -> Envelope {
    new_notification("disconnect", Map::new())
}

/// Decode a WebSocket text frame into a JSON value.
pub fn decode_message(text: &str) -> Result<Value, serde_json::Error> {
    // Support rare NDJSON frames: take first non-empty line if multiple.
    let trimmed = text.trim();
    if trimmed.contains('\n') {
        for line in trimmed.lines() {
            let line = line.trim();
            if !line.is_empty() {
                return serde_json::from_str(line);
            }
        }
    }
    serde_json::from_str(trimmed)
}

/// Expand `event_batch` frames into individual events.
pub fn expand_wire_messages(msg: Value) -> Vec<Value> {
    let Some(obj) = msg.as_object() else {
        return vec![msg];
    };
    if obj.get("type").and_then(|v| v.as_str()) != Some("event_batch") {
        return vec![msg];
    }
    match obj.get("events").and_then(|v| v.as_array()) {
        Some(events) if !events.is_empty() => events.clone(),
        _ => vec![msg],
    }
}

/// Unwrap a `next` envelope to its inner frame when possible.
///
/// Matches Go `appkit.UnwrapNext`: when `type==next` and `payload.data` is an
/// object, return that object; otherwise return the original message.
pub fn unwrap_next(msg: &Value) -> Value {
    let Some(obj) = msg.as_object() else {
        return msg.clone();
    };
    if obj.get("type").and_then(|v| v.as_str()) != Some("next") {
        return msg.clone();
    }
    let Some(payload) = obj.get("payload").and_then(|p| p.as_object()) else {
        return msg.clone();
    };
    if let Some(data) = payload.get("data") {
        if data.is_object() {
            return data.clone();
        }
    }
    msg.clone()
}

/// Coerce JSON value to string.
pub fn as_str(v: &Value) -> &str {
    v.as_str().unwrap_or("")
}

/// Map helper: insert string.
pub fn params_map(pairs: &[(&str, Value)]) -> Map<String, Value> {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert((*k).to_string(), v.clone());
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_id_is_32_hex() {
        let id = new_request_id();
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn connection_init_shape() {
        let env = new_connection_init();
        assert_eq!(env.msg_type, "connection_init");
        assert_eq!(env.proto, "1");
        let params = env.params.unwrap();
        assert_eq!(params["client_name"], json!("soothe-client-rust"));
    }

    #[test]
    fn expand_event_batch() {
        let batch = json!({
            "type": "event_batch",
            "events": [{"type":"event","mode":"messages"}, {"type":"status","state":"idle"}]
        });
        let expanded = expand_wire_messages(batch);
        assert_eq!(expanded.len(), 2);
    }

    #[test]
    fn unwrap_next_returns_payload_data() {
        let frame = json!({
            "type": "next",
            "payload": {
                "mode": "event",
                "data": {"type": "event", "mode": "messages", "data": [{"content": "hi"}]}
            }
        });
        let inner = unwrap_next(&frame);
        assert_eq!(inner["mode"], json!("messages"));
        assert_eq!(inner["type"], json!("event"));
    }
}
