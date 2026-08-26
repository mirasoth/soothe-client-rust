//! Fire-and-forget send variants (Go `send_methods.go` parity).
//!
//! Each `send_*` method emits the protocol-1 envelope and returns immediately
//! without waiting for a response. Callers that need the response should use
//! the blocking convenience methods on [`crate::Client`] (e.g. `loop_get`,
//! `job_status`, `autopilot_status`).

use serde_json::{json, Map, Value};

use crate::client::Client;
use crate::errors::Result;
use crate::protocol::{new_disconnect, new_request_with_id, new_subscribe};

/// Return the first non-empty request id from the variadic args, or empty
/// (signalling the caller to generate one) when none is provided.
fn opt_request_id(request_id: &[&str]) -> String {
    request_id
        .iter()
        .copied()
        .find(|s| !s.is_empty())
        .unwrap_or_default()
        .to_string()
}

impl Client {
    /// Fire `slash_command` notification (Go `SendCommand` parity).
    pub async fn send_command(&self, cmd: &str) -> Result<()> {
        let mut params = Map::new();
        params.insert("cmd".into(), json!(cmd));
        self.notify("slash_command", params).await
    }

    /// Fire `disconnect` notification (Go `SendDetach` parity).
    pub async fn send_detach(&self) -> Result<()> {
        self.send_envelope(new_disconnect()).await
    }

    /// Send `daemon_status` request envelope (Go `SendDaemonStatus` parity).
    pub async fn send_daemon_status(&self, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        self.send_envelope(new_request_with_id("daemon_status", Map::new(), rid))
            .await
    }

    /// Send `daemon_shutdown` request envelope (Go `SendDaemonShutdown` parity).
    pub async fn send_daemon_shutdown(&self, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        self.send_envelope(new_request_with_id("daemon_shutdown", Map::new(), rid))
            .await
    }

    /// Send `config_get` request envelope (Go `SendConfigGet` parity).
    pub async fn send_config_get(&self, section: &str, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("section".into(), json!(section));
        self.send_envelope(new_request_with_id("config_get", params, rid))
            .await
    }

    /// Send `config_reload` request envelope (Go `SendConfigReload` parity).
    pub async fn send_config_reload(&self, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        self.send_envelope(new_request_with_id("config_reload", Map::new(), rid))
            .await
    }

    /// Send `skills_list` request envelope (Go `SendSkillsList` parity).
    pub async fn send_skills_list(&self, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        self.send_envelope(new_request_with_id("skills_list", Map::new(), rid))
            .await
    }

    /// Send `models_list` request envelope (Go `SendModelsList` parity).
    pub async fn send_models_list(&self, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        self.send_envelope(new_request_with_id("models_list", Map::new(), rid))
            .await
    }

    /// Send `invoke_skill` request envelope (Go `SendInvokeSkill` parity).
    pub async fn send_invoke_skill(
        &self,
        skill: &str,
        args: &str,
        interaction_mode: Option<&str>,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("skill".into(), json!(skill));
        if !args.is_empty() {
            params.insert("args".into(), json!(args));
        }
        if let Some(v) = interaction_mode {
            params.insert("interaction_mode".into(), json!(v));
        }
        self.send_envelope(new_request_with_id("invoke_skill", params, rid))
            .await
    }

    /// Send `mcp_status` request envelope (Go `SendMCPStatus` parity).
    pub async fn send_mcp_status(&self, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        self.send_envelope(new_request_with_id("mcp_status", Map::new(), rid))
            .await
    }

    /// Send `loop_list` request envelope (Go `SendLoopList` parity).
    pub async fn send_loop_list(
        &self,
        filter: Option<Map<String, Value>>,
        limit: u32,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        if let Some(f) = filter {
            params.insert("filter".into(), Value::Object(f));
        }
        if limit > 0 {
            params.insert("limit".into(), json!(limit));
        }
        self.send_envelope(new_request_with_id("loop_list", params, rid))
            .await
    }

    /// Send `loop_get` request envelope (Go `SendLoopGet` parity).
    pub async fn send_loop_get(
        &self,
        loop_id: &str,
        verbose: bool,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        if verbose {
            params.insert("verbose".into(), json!(true));
        }
        self.send_envelope(new_request_with_id("loop_get", params, rid))
            .await
    }

    /// Send `loop_tree` request envelope (Go `SendLoopTree` parity).
    pub async fn send_loop_tree(
        &self,
        loop_id: &str,
        format: &str,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        if !format.is_empty() {
            params.insert("format".into(), json!(format));
        }
        self.send_envelope(new_request_with_id("loop_tree", params, rid))
            .await
    }

    /// Send `loop_prune` request envelope (Go `SendLoopPrune` parity).
    pub async fn send_loop_prune(
        &self,
        loop_id: &str,
        keep_latest: u32,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        if keep_latest > 0 {
            params.insert("keep_latest".into(), json!(keep_latest));
        }
        self.send_envelope(new_request_with_id("loop_prune", params, rid))
            .await
    }

    /// Send `loop_delete` request envelope (Go `SendLoopDelete` parity).
    pub async fn send_loop_delete(&self, loop_id: &str, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        self.send_envelope(new_request_with_id("loop_delete", params, rid))
            .await
    }

    /// Send `loop_reattach` request envelope (Go `SendLoopReattach` parity).
    pub async fn send_loop_reattach(&self, loop_id: &str, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        self.send_envelope(new_request_with_id("loop_reattach", params, rid))
            .await
    }

    /// Send `loop_events` subscribe envelope (Go `SendLoopSubscribe` parity).
    ///
    /// Pass empty `wire_tier` / `stream_delivery` to omit those fields.
    pub async fn send_loop_subscribe(
        &self,
        loop_id: &str,
        wire_tier: &str,
        stream_delivery: &str,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        if !wire_tier.is_empty() {
            params.insert("wire_tier".into(), json!(wire_tier));
        }
        if !stream_delivery.is_empty() {
            params.insert("stream_delivery".into(), json!(stream_delivery));
        }
        let env = if rid.is_empty() {
            new_subscribe("loop_events", params)
        } else {
            // Explicit id: build subscribe with that id for caller correlation.
            let mut e = new_subscribe("loop_events", params);
            e.id = Some(rid);
            e
        };
        self.send_envelope(env).await
    }

    /// Send unsubscribe by subscription id (Go `SendLoopDetach` parity).
    ///
    /// When `request_id` is empty, `loop_id` is used as the subscription id.
    pub async fn send_loop_detach(&self, loop_id: &str, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        let id = if rid.is_empty() {
            loop_id.to_string()
        } else {
            rid
        };
        self.unsubscribe(&id).await
    }

    /// Send `loop_new` request envelope (Go `SendLoopNew` parity).
    pub async fn send_loop_new(
        &self,
        client_workspace: &str,
        user_id: &str,
        client_workspace_id: &str,
        is_ephemeral: bool,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        if !client_workspace.is_empty() {
            params.insert("client_workspace".into(), json!(client_workspace));
        }
        if !user_id.is_empty() {
            params.insert("user_id".into(), json!(user_id));
        }
        if !client_workspace_id.is_empty() {
            params.insert("client_workspace_id".into(), json!(client_workspace_id));
        }
        if is_ephemeral {
            params.insert("is_ephemeral".into(), json!(true));
        }
        self.send_envelope(new_request_with_id("loop_new", params, rid))
            .await
    }

    /// Send `loop_input` notification (Go `SendLoopInput` parity).
    pub async fn send_loop_input(&self, loop_id: &str, content: &str) -> Result<()> {
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        params.insert("content".into(), json!(content));
        self.notify("loop_input", params).await
    }

    /// Send `loop_messages` request envelope (Go `SendLoopMessages` parity).
    pub async fn send_loop_messages(
        &self,
        loop_id: &str,
        limit: u32,
        offset: u32,
        include_events: bool,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        if limit > 0 {
            params.insert("limit".into(), json!(limit));
        }
        if offset > 0 {
            params.insert("offset".into(), json!(offset));
        }
        if include_events {
            params.insert("include_events".into(), json!(true));
        }
        self.send_envelope(new_request_with_id("loop_messages", params, rid))
            .await
    }

    /// Send `loop_state_get` request envelope (Go `SendLoopStateGet` parity).
    pub async fn send_loop_state_get(&self, loop_id: &str, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        self.send_envelope(new_request_with_id("loop_state_get", params, rid))
            .await
    }

    /// Send `loop_state_update` request envelope (Go `SendLoopStateUpdate` parity).
    pub async fn send_loop_state_update(
        &self,
        loop_id: &str,
        values: Map<String, Value>,
        as_node: &str,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        params.insert("values".into(), Value::Object(values));
        if !as_node.is_empty() {
            params.insert("as_node".into(), json!(as_node));
        }
        self.send_envelope(new_request_with_id("loop_state_update", params, rid))
            .await
    }

    /// Send `loop_history_fetch` request envelope (Go `SendLoopHistoryFetch` parity).
    pub async fn send_loop_history_fetch(&self, loop_id: &str, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        self.send_envelope(new_request_with_id("loop_history_fetch", params, rid))
            .await
    }

    /// Send `loop_execution_state_fetch` request envelope
    /// (Go `SendLoopExecutionStateFetch` parity).
    pub async fn send_loop_execution_state_fetch(
        &self,
        loop_id: &str,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("loop_id".into(), json!(loop_id));
        self.send_envelope(new_request_with_id(
            "loop_execution_state_fetch",
            params,
            rid,
        ))
        .await
    }

    /// Send `auth` request envelope (Go `SendAuth` parity).
    pub async fn send_auth(
        &self,
        access_key: &str,
        secret_key: &str,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("access_key".into(), json!(access_key));
        params.insert("secret_key".into(), json!(secret_key));
        self.send_envelope(new_request_with_id("auth", params, rid))
            .await
    }

    /// Send `auth_refresh` request envelope (Go `SendAuthRefresh` parity).
    pub async fn send_auth_refresh(&self, refresh_token: &str, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("refresh_token".into(), json!(refresh_token));
        self.send_envelope(new_request_with_id("auth_refresh", params, rid))
            .await
    }

    /// Send `cron_add` request envelope (Go `SendCronAdd` parity).
    pub async fn send_cron_add(
        &self,
        text: &str,
        priority: i32,
        request_id: &[&str],
    ) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("text".into(), json!(text));
        if priority > 0 {
            params.insert("priority".into(), json!(priority));
        }
        self.send_envelope(new_request_with_id("cron_add", params, rid))
            .await
    }

    /// Send `cron_list` request envelope (Go `SendCronList` parity).
    pub async fn send_cron_list(&self, status: &str, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        if !status.is_empty() {
            params.insert("status".into(), json!(status));
        }
        self.send_envelope(new_request_with_id("cron_list", params, rid))
            .await
    }

    /// Send `cron_show` request envelope (Go `SendCronShow` parity).
    pub async fn send_cron_show(&self, job_id: &str, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.send_envelope(new_request_with_id("cron_show", params, rid))
            .await
    }

    /// Send `cron_cancel` request envelope (Go `SendCronCancel` parity).
    pub async fn send_cron_cancel(&self, job_id: &str, request_id: &[&str]) -> Result<()> {
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.send_envelope(new_request_with_id("cron_cancel", params, rid))
            .await
    }

    /// Fire-and-forget `job_create` (Go `SendJobCreate` parity).
    pub async fn send_job_create(
        &self,
        goal: &str,
        workspace: Option<&str>,
        request_id: &[&str],
    ) -> Result<()> {
        if goal.is_empty() {
            return Err(crate::errors::Error::msg("goal is required"));
        }
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("goal".into(), json!(goal));
        if let Some(ws) = workspace {
            if !ws.is_empty() {
                params.insert("workspace".into(), json!(ws));
            }
        }
        self.send_envelope(new_request_with_id("job_create", params, rid))
            .await
    }

    /// Fire-and-forget `job_status` (Go `SendJobStatus` parity).
    pub async fn send_job_status(&self, job_id: &str, request_id: &[&str]) -> Result<()> {
        if job_id.is_empty() {
            return Err(crate::errors::Error::msg("job_id is required"));
        }
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.send_envelope(new_request_with_id("job_status", params, rid))
            .await
    }

    /// Fire-and-forget `job_pause` (Go `SendJobPause` parity).
    pub async fn send_job_pause(&self, job_id: &str, request_id: &[&str]) -> Result<()> {
        if job_id.is_empty() {
            return Err(crate::errors::Error::msg("job_id is required"));
        }
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.send_envelope(new_request_with_id("job_pause", params, rid))
            .await
    }

    /// Fire-and-forget `job_resume` (Go `SendJobResume` parity).
    pub async fn send_job_resume(&self, job_id: &str, request_id: &[&str]) -> Result<()> {
        if job_id.is_empty() {
            return Err(crate::errors::Error::msg("job_id is required"));
        }
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.send_envelope(new_request_with_id("job_resume", params, rid))
            .await
    }

    /// Fire-and-forget `job_cancel` (Go `SendJobCancel` parity).
    pub async fn send_job_cancel(&self, job_id: &str, request_id: &[&str]) -> Result<()> {
        if job_id.is_empty() {
            return Err(crate::errors::Error::msg("job_id is required"));
        }
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.send_envelope(new_request_with_id("job_cancel", params, rid))
            .await
    }

    /// Fire-and-forget `job_dag` (Go `SendJobDag` parity).
    pub async fn send_job_dag(&self, job_id: &str, request_id: &[&str]) -> Result<()> {
        if job_id.is_empty() {
            return Err(crate::errors::Error::msg("job_id is required"));
        }
        let rid = opt_request_id(request_id);
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.send_envelope(new_request_with_id("job_dag", params, rid))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opt_request_id_picks_first_non_empty() {
        assert_eq!(opt_request_id(&[]), "");
        assert_eq!(opt_request_id(&[""]), "");
        assert_eq!(opt_request_id(&["abc"]), "abc");
        assert_eq!(opt_request_id(&["", "def"]), "def");
    }
}
