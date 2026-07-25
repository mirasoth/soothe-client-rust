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
    /// Protocol version (defaults to empty when omitted by legacy frames).
    #[serde(default)]
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

/// Build a request envelope with a generated id.
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

/// Build a request envelope with an explicit id.
///
/// When `id` is empty, a new id is generated (Go `NewRequestEnvelopeWithID` parity).
pub fn new_request_with_id(
    method: impl Into<String>,
    params: Map<String, Value>,
    id: impl Into<String>,
) -> Envelope {
    let id = id.into();
    let id = if id.trim().is_empty() {
        new_request_id()
    } else {
        id
    };
    Envelope {
        proto: PROTO_VERSION.to_string(),
        msg_type: "request".into(),
        method: Some(method.into()),
        params: if params.is_empty() {
            None
        } else {
            Some(params)
        },
        id: Some(id),
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

/// Split a WebSocket text payload into one or more JSON objects.
///
/// The daemon may send newline-delimited JSON (NDJSON) in a single frame.
/// Each non-empty trimmed line is returned as a separate byte slice (Go
/// `SplitSootheWirePayload` parity).
pub fn split_wire_payload(data: &str) -> Vec<&str> {
    let trimmed = data.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let mut out: Vec<&str> = trimmed
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();
    if out.is_empty() {
        out.push(trimmed);
    }
    out
}

/// Common message base with type and optional request_id.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct BaseMessage {
    /// Optional request id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Frame type (`event`, `status`, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[allow(missing_docs)]
    pub r#type: Option<String>,
}

/// Streaming event frame from the daemon (Go `EventMessage` parity).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct EventMessage {
    /// Base message fields.
    #[serde(flatten)]
    pub base: BaseMessage,
    /// Loop id this event belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loop_id: Option<String>,
    /// Event namespace (string or list form).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<Value>,
    /// Stream mode (`messages`, `updates`, `custom`, `event`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Event payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    /// Optional timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

impl EventMessage {
    /// Normalized event type: prefer `data.type`, fall back to `namespace` as string.
    pub fn event_type(&self) -> String {
        if let Some(data) = self.data.as_ref().and_then(|d| d.as_object()) {
            if let Some(t) = data.get("type").and_then(|v| v.as_str()) {
                if !t.is_empty() {
                    return t.to_string();
                }
            }
        }
        if let Some(s) = self.namespace.as_ref().and_then(|n| n.as_str()) {
            if !s.is_empty() {
                return s.to_string();
            }
        }
        String::new()
    }

    /// Namespace path segments regardless of wire encoding (string or list).
    pub fn namespace_parts(&self) -> Vec<String> {
        match &self.namespace {
            Some(Value::String(s)) if !s.is_empty() => {
                s.split('.').map(|p| p.to_string()).collect()
            }
            Some(Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Extract a loop-tagged assistant message from `mode=messages` events.
    ///
    /// Returns `None` for non-message events or non-assistant payloads.
    pub fn loop_ai_message(&self) -> Option<LoopAIMessage> {
        if self.mode.as_deref() != Some("messages") {
            return None;
        }
        let items = self.data.as_ref().and_then(|d| d.as_array())?;
        let first = items.first()?;
        let msg_map = first.as_object()?;
        let phase_str = msg_map.get("phase").and_then(|v| v.as_str()).unwrap_or("");
        if phase_str.is_empty() {
            return None;
        }
        if !is_loop_assistant_phase(phase_str) {
            return None;
        }
        let type_str = msg_map.get("type").and_then(|v| v.as_str()).unwrap_or("ai");
        let msg = LoopAIMessage {
            r#type: Some(type_str.to_string()),
            content: msg_map.get("content").cloned(),
            phase: Some(phase_str.to_string()),
        };
        Some(msg)
    }
}

/// Loop-tagged assistant payload forwarded on `mode=messages` streams
/// (Go `LoopAIMessage` parity).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct LoopAIMessage {
    /// Message type (typically `ai`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[allow(missing_docs)]
    pub r#type: Option<String>,
    /// Message content (string, array of blocks, or object).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<Value>,
    /// Stream phase (`text_completion`, `goal_completion`, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
}

impl LoopAIMessage {
    /// Extract plain text from the content payload.
    pub fn loop_ai_text(&self) -> String {
        match &self.content {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Array(items)) => {
                let mut buf = String::new();
                for item in items {
                    if let Some(s) = item.as_str() {
                        buf.push_str(s);
                        continue;
                    }
                    if let Some(blk) = item.as_object() {
                        if let Some(t) = blk.get("text").and_then(|v| v.as_str()) {
                            buf.push_str(t);
                        }
                    }
                }
                buf
            }
            Some(Value::Object(obj)) => obj
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            _ => String::new(),
        }
    }
}

/// Status acknowledgment (Go `StatusResponse` parity). Handles the camelCase
/// `loopId` fallback emitted by some daemon builds.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct StatusResponse {
    /// Base message fields.
    #[serde(flatten)]
    pub base: BaseMessage,
    /// Daemon state (`running`, `idle`, `stopped`, `detached`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Loop id (snake_case form).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loop_id: Option<String>,
    /// CamelCase fallback (`loopId`) emitted by some daemon builds.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "loopId")]
    pub loop_id_camel: Option<String>,
    /// Workspace path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
}

impl StatusResponse {
    /// Resolved loop id (prefers snake_case, falls back to camelCase).
    pub fn resolved_loop_id(&self) -> Option<&str> {
        self.loop_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .or_else(|| self.loop_id_camel.as_deref().filter(|s| !s.is_empty()))
    }
}

/// True when `phase` tags streamable loop assistant output.
///
/// Broader than `DEFAULT_DELIVERABLE_PHASES` (which only lists phases that may
/// end a turn) — this includes narration phases like `plan_direct`.
pub fn is_loop_assistant_phase(phase: &str) -> bool {
    matches!(
        phase,
        "goal_completion"
            | "quiz"
            | "autonomous_goal"
            | "text_completion"
            | "image_to_text"
            | "ocr"
            | "embed"
            | "plan_direct"
            | "chitchat"
    )
}

/// Decode a WebSocket text frame into a typed message (Go `DecodeMessage` parity).
///
/// Dispatches by the `type` field:
/// - Protocol-1 envelope types → `Envelope`
/// - `next` whose payload wraps an event-shaped frame → `EventMessage`
/// - `event` → `EventMessage`
/// - `status` → `StatusResponse` (with camelCase `loopId` fallback)
/// - `event_batch` → raw `Value` (use [`expand_wire_messages`] to flatten)
/// - Unknown → raw `Value`
pub fn decode_message_typed(text: &str) -> Result<TypedMessage, serde_json::Error> {
    let value = decode_message(text)?;
    Ok(TypedMessage::from_value(value))
}

/// Typed decoded message (Go `interface{}` return parity).
#[derive(Debug, Clone, PartialEq)]
pub enum TypedMessage {
    /// Protocol-1 envelope.
    Envelope(Envelope),
    /// Streaming event frame (from `event` or projected `next`).
    Event(EventMessage),
    /// Status frame.
    Status(StatusResponse),
    /// Raw value (unknown type or `event_batch`).
    Value(Value),
}

impl TypedMessage {
    /// Dispatch a decoded JSON value into the typed variant.
    pub fn from_value(value: Value) -> Self {
        let Some(t) = value.get("type").and_then(|v| v.as_str()) else {
            return Self::Value(value);
        };
        match t {
            "connection_init" | "connection_ack" | "request" | "response" | "notification"
            | "subscribe" | "error" | "complete" | "unsubscribe" | "ping" | "pong"
            | "receipt_response" | "disconnect" => {
                match serde_json::from_value::<Envelope>(value.clone()) {
                    Ok(env) => Self::Envelope(env),
                    Err(_) => Self::Value(value),
                }
            }
            "next" => match serde_json::from_value::<Envelope>(value.clone()) {
                Ok(env) => match next_to_event_message(&env) {
                    Some(em) => Self::Event(em),
                    None => Self::Envelope(env),
                },
                Err(_) => Self::Value(value),
            },
            "event" => match serde_json::from_value::<EventMessage>(value.clone()) {
                Ok(em) => Self::Event(em),
                Err(_) => Self::Value(value),
            },
            "status" => match serde_json::from_value::<StatusResponse>(value.clone()) {
                Ok(s) => Self::Status(s),
                Err(_) => Self::Value(value),
            },
            _ => Self::Value(value),
        }
    }
}

/// Project a protocol-1 `next` envelope's payload into an [`EventMessage`] when
/// the payload carries a wrapped legacy event frame (Go `nextToEventMessage` parity).
///
/// The daemon wraps legacy free-form frames as
/// `{payload:{namespace, mode:<orig type>, data:<orig frame>}}`. The original
/// event frame (with its own type/mode/data/loop_id) lives inside `payload.data`,
/// so we project from the inner frame to preserve the legacy `EventMessage`
/// shape (Mode, Data, LoopID, Namespace) that consumers expect.
pub fn next_to_event_message(env: &Envelope) -> Option<EventMessage> {
    let payload = env.payload.as_ref()?.as_object()?;
    // Common case: payload.data is the original event frame.
    if let Some(inner) = payload.get("data").and_then(|d| d.as_object()) {
        let inner_type = inner.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if (inner_type == "event" || inner_type.is_empty()) && inner.contains_key("mode") {
            let em = EventMessage {
                base: BaseMessage {
                    r#type: Some("event".to_string()),
                    request_id: None,
                },
                loop_id: inner
                    .get("loop_id")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        inner
                            .get("loopId")
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                    }),
                namespace: inner.get("namespace").cloned(),
                mode: inner
                    .get("mode")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                data: inner.get("data").cloned(),
                timestamp: None,
            };
            return Some(em);
        }
    }
    // Fallback: payload itself carries mode/data directly.
    let has_mode = payload.contains_key("mode");
    let has_data = payload.contains_key("data");
    if has_mode || has_data {
        let em = EventMessage {
            base: BaseMessage {
                r#type: Some("event".to_string()),
                request_id: None,
            },
            loop_id: payload
                .get("loop_id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string()),
            namespace: payload.get("namespace").cloned(),
            mode: payload
                .get("mode")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            data: payload.get("data").cloned(),
            timestamp: None,
        };
        return Some(em);
    }
    None
}

/// Extract a loop id from a decoded message (Go `ExtractSootheLoopID` parity).
///
/// Handles protocol-1 envelopes (next.payload.data.loop_id, status.params.loop_id),
/// [`EventMessage`], [`StatusResponse`], and raw maps (including camelCase `loopId`).
pub fn extract_soothe_loop_id(msg: &Value) -> Option<String> {
    let obj = msg.as_object()?;
    let t = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
    match t {
        "next" => {
            let payload = obj.get("payload").and_then(|p| p.as_object())?;
            if let Some(data) = payload.get("data").and_then(|d| d.as_object()) {
                if let Some(s) = data
                    .get("loop_id")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                {
                    return Some(s.to_string());
                }
                if let Some(s) = data
                    .get("loopId")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                {
                    return Some(s.to_string());
                }
            }
            payload
                .get("loop_id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        }
        "status" => obj
            .get("params")
            .and_then(|p| p.get("loop_id"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .or_else(|| {
                obj.get("loop_id")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
            })
            .or_else(|| {
                obj.get("loopId")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
            }),
        _ => obj
            .get("loop_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .or_else(|| {
                obj.get("loopId")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
            }),
    }
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

    #[test]
    fn new_request_with_id_explicit() {
        let env = new_request_with_id("loop_get", Map::new(), "abc123");
        assert_eq!(env.id.as_deref(), Some("abc123"));
        assert_eq!(env.msg_type, "request");
    }

    #[test]
    fn new_request_with_id_empty_generates() {
        let env = new_request_with_id("loop_get", Map::new(), "");
        assert!(env.id.is_some());
        assert!(!env.id.unwrap().is_empty());
    }

    #[test]
    fn split_ndjson_payload() {
        let line1 = r#"{"type":"status","state":"running"}"#;
        let line2 = r#"{"type":"event","mode":"messages"}"#;
        let data = format!("{line1}\n{line2}");
        let frames = split_wire_payload(&data);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0], line1);
        assert_eq!(frames[1], line2);
    }

    #[test]
    fn split_single_frame_payload() {
        let data = r#"{"type":"status","state":"idle"}"#;
        let frames = split_wire_payload(data);
        assert_eq!(frames.len(), 1);
    }

    #[test]
    fn split_empty_payload() {
        assert!(split_wire_payload("").is_empty());
        assert!(split_wire_payload("   \n  \n").is_empty());
    }

    #[test]
    fn decode_message_typed_status() {
        let text = r#"{"type":"status","state":"running","loop_id":"abc"}"#;
        let typed = decode_message_typed(text).unwrap();
        match typed {
            TypedMessage::Status(s) => {
                assert_eq!(s.state.as_deref(), Some("running"));
                assert_eq!(s.resolved_loop_id(), Some("abc"));
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[test]
    fn decode_message_typed_status_camel_loop_id() {
        let text = r#"{"type":"status","state":"idle","loopId":"xyz"}"#;
        let typed = decode_message_typed(text).unwrap();
        match typed {
            TypedMessage::Status(s) => {
                assert_eq!(s.resolved_loop_id(), Some("xyz"));
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[test]
    fn decode_message_typed_next_projects_event() {
        let text = r#"{"type":"next","payload":{"mode":"event","data":{"type":"event","mode":"messages","data":[{"content":"hi"}],"loop_id":"L1"}}}"#;
        let typed = decode_message_typed(text).unwrap();
        match typed {
            TypedMessage::Event(em) => {
                assert_eq!(em.mode.as_deref(), Some("messages"));
                assert_eq!(em.loop_id.as_deref(), Some("L1"));
            }
            other => panic!("expected Event, got {other:?}"),
        }
    }

    #[test]
    fn decode_message_typed_event() {
        let text = r#"{"type":"event","mode":"custom","data":{"type":"foo"}}"#;
        let typed = decode_message_typed(text).unwrap();
        match typed {
            TypedMessage::Event(em) => {
                assert_eq!(em.mode.as_deref(), Some("custom"));
                assert_eq!(em.event_type(), "foo");
            }
            other => panic!("expected Event, got {other:?}"),
        }
    }

    #[test]
    fn decode_message_typed_envelope() {
        let text = r#"{"proto":"1","type":"response","id":"r1","result":{}}"#;
        let typed = decode_message_typed(text).unwrap();
        assert!(matches!(typed, TypedMessage::Envelope(_)));
    }

    #[test]
    fn extract_loop_id_from_next() {
        let v = json!({
            "type": "next",
            "payload": {"data": {"loop_id": "L99"}}
        });
        assert_eq!(extract_soothe_loop_id(&v).as_deref(), Some("L99"));
    }

    #[test]
    fn extract_loop_id_camel_case() {
        let v = json!({"type": "status", "loopId": "Camel"});
        assert_eq!(extract_soothe_loop_id(&v).as_deref(), Some("Camel"));
    }

    #[test]
    fn loop_ai_text_from_string() {
        let m = LoopAIMessage {
            r#type: Some("ai".into()),
            content: Some(json!("hello")),
            phase: Some("text_completion".into()),
        };
        assert_eq!(m.loop_ai_text(), "hello");
    }

    #[test]
    fn loop_ai_text_from_block_array() {
        let m = LoopAIMessage {
            r#type: Some("ai".into()),
            content: Some(json!([{"text": "block1"}, " ", {"text": "block2"}])),
            phase: Some("text_completion".into()),
        };
        assert_eq!(m.loop_ai_text(), "block1 block2");
    }

    #[test]
    fn loop_assistant_phase_broad() {
        assert!(is_loop_assistant_phase("plan_direct"));
        assert!(is_loop_assistant_phase("text_completion"));
        assert!(!is_loop_assistant_phase("direct_llm"));
    }
}
