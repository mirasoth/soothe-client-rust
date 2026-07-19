//! TurnRunner: single-flight execute over ConnectionPool.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::client::SendInputOptions;
use crate::errors::{Error, Result};

use super::classifier::EventClassifier;
use super::pool::{input_message_for_loop, ConnectionPool};
use super::query_gate::QueryGate;
use super::session_store::SessionStore;

/// Timeout policy for idle / query / stream-close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeoutPolicy {
    /// Fail the turn.
    #[default]
    Fail,
    /// Soft-complete with whatever was collected.
    SoftComplete,
}

/// Turn runner configuration.
#[derive(Debug, Clone)]
pub struct TurnConfig {
    /// Absolute query timeout.
    pub query_timeout: Duration,
    /// Idle silence timeout (0 = off).
    pub idle_timeout: Duration,
    /// Idle/query/stream policies.
    pub on_idle_timeout: TimeoutPolicy,
    /// Query timeout policy.
    pub on_query_timeout: TimeoutPolicy,
    /// Stream close policy.
    pub on_stream_close: TimeoutPolicy,
}

impl Default for TurnConfig {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(30 * 60),
            idle_timeout: Duration::ZERO,
            on_idle_timeout: TimeoutPolicy::Fail,
            on_query_timeout: TimeoutPolicy::Fail,
            on_stream_close: TimeoutPolicy::Fail,
        }
    }
}

/// Optional input knobs for TurnRunner.
#[derive(Debug, Clone, Default)]
pub struct InputOpts {
    /// Intent hint.
    pub intent_hint: Option<String>,
    /// Preferred subagent.
    pub preferred_subagent: Option<String>,
    /// Response schema.
    pub response_schema: Option<Value>,
    /// Schema name.
    pub response_schema_name: Option<String>,
    /// Strict schema.
    pub response_schema_strict: Option<bool>,
}

/// Executes a turn against a pooled connection.
pub struct TurnRunner<S: SessionStore> {
    pool: Arc<ConnectionPool<S>>,
    gate: QueryGate,
    classifier: EventClassifier,
    store: Arc<S>,
    cfg: TurnConfig,
}

impl<S: SessionStore + 'static> TurnRunner<S> {
    /// Create a runner.
    pub fn new(
        pool: Arc<ConnectionPool<S>>,
        gate: QueryGate,
        classifier: EventClassifier,
        store: Arc<S>,
        cfg: Option<TurnConfig>,
    ) -> Self {
        Self {
            pool,
            gate,
            classifier,
            store,
            cfg: cfg.unwrap_or_default(),
        }
    }

    /// Execute one turn for a session; returns concatenated assistant text when found.
    pub async fn execute(
        &self,
        session_id: &str,
        message: &str,
        user_id: &str,
        workspace_id: &str,
        attachments: Option<Value>,
        opts: Option<InputOpts>,
    ) -> Result<String> {
        self.gate
            .acquire(session_id)
            .await
            .map_err(|e| Error::msg(e.to_string()))?;
        let result = self
            .execute_inner(
                session_id,
                message,
                user_id,
                workspace_id,
                attachments,
                opts,
            )
            .await;
        self.gate.release(session_id).await;
        result
    }

    async fn execute_inner(
        &self,
        session_id: &str,
        message: &str,
        user_id: &str,
        workspace_id: &str,
        attachments: Option<Value>,
        opts: Option<InputOpts>,
    ) -> Result<String> {
        let conn = self.pool.acquire(session_id, workspace_id, user_id).await?;
        let loop_id = conn.loop_id.clone();
        let flat = input_message_for_loop(message, &loop_id, attachments.clone(), opts.as_ref());
        // Coerce flat → notification params.
        let mut params = Map::new();
        for (k, v) in flat {
            if k == "type" {
                continue;
            }
            params.insert(k, v);
        }
        let input_opts = SendInputOptions {
            loop_id: Some(loop_id.clone()),
            intent_hint: opts.as_ref().and_then(|o| o.intent_hint.clone()),
            preferred_subagent: opts.as_ref().and_then(|o| o.preferred_subagent.clone()),
            response_schema: opts.as_ref().and_then(|o| o.response_schema.clone()),
            response_schema_name: opts.as_ref().and_then(|o| o.response_schema_name.clone()),
            response_schema_strict: opts.as_ref().and_then(|o| o.response_schema_strict),
            attachments,
            ..Default::default()
        };
        // Prefer typed send_input.
        let _ = params;
        conn.client.send_input(message, input_opts).await?;

        self.store
            .append_message(session_id, json!({"role":"user","content": message}))
            .await;

        let deadline = tokio::time::Instant::now() + self.cfg.query_timeout;
        let mut collected = String::new();
        let mut last_event = tokio::time::Instant::now();
        let mut boundary = super::turn_boundary::TurnBoundary::default();

        loop {
            if tokio::time::Instant::now() > deadline {
                self.pool.release(session_id).await;
                return match self.cfg.on_query_timeout {
                    TimeoutPolicy::SoftComplete => Ok(collected),
                    TimeoutPolicy::Fail => Err(Error::msg("query timeout")),
                };
            }
            if !self.cfg.idle_timeout.is_zero() && last_event.elapsed() > self.cfg.idle_timeout {
                self.pool.release(session_id).await;
                return match self.cfg.on_idle_timeout {
                    TimeoutPolicy::SoftComplete => Ok(collected),
                    TimeoutPolicy::Fail => Err(Error::msg("idle timeout")),
                };
            }

            let wait = if self.cfg.idle_timeout.is_zero() {
                Duration::from_millis(500)
            } else {
                self.cfg.idle_timeout.min(Duration::from_millis(500))
            };
            let ev = conn.client.read_event_with_timeout(wait).await?;
            let Some(ev) = ev else {
                if !conn.client.is_connection_alive() {
                    self.pool.release(session_id).await;
                    return match self.cfg.on_stream_close {
                        TimeoutPolicy::SoftComplete => Ok(collected),
                        TimeoutPolicy::Fail => Err(Error::msg("stream closed")),
                    };
                }
                continue;
            };
            last_event = tokio::time::Instant::now();

            let frame = if ev.get("type").and_then(|v| v.as_str()) == Some("next") {
                crate::client::unwrap_next_frame(&ev)
            } else {
                ev
            };
            let event_type = frame.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if event_type == "status" {
                let state = frame.get("state").and_then(|v| v.as_str()).unwrap_or("");
                if boundary.feed_status(state).is_some() {
                    break;
                }
                continue;
            }
            if event_type != "event" {
                continue;
            }
            let mode = frame.get("mode").and_then(|v| v.as_str()).unwrap_or("");
            let data = frame.get("data").cloned().unwrap_or(Value::Null);
            if let Some(text) = self.classifier.extract_text(mode, &data) {
                collected.push_str(&text);
            }
            // TurnBoundary owns stream.end / idle / stopped (DaemonSession parity).
            if boundary.feed_event(mode, &data).is_some() {
                break;
            }
        }

        if !collected.is_empty() {
            self.store
                .append_message(session_id, json!({"role":"assistant","content": collected}))
                .await;
        }
        self.pool.release(session_id).await;
        Ok(collected)
    }
}
