//! Ephemeral command clients for jobs / cron / autopilot one-shots.

use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::client::Client;
use crate::errors::Result;
use crate::session::connect_with_retries;

/// Async one-shot RPC client (connect → RPC → close).
#[derive(Debug, Clone)]
pub struct AsyncCommandClient {
    ws_url: String,
    timeout: Duration,
}

impl AsyncCommandClient {
    /// Create against `ws_url` with default 30s timeout.
    pub fn new(ws_url: impl Into<String>) -> Self {
        Self {
            ws_url: ws_url.into(),
            timeout: Duration::from_secs(30),
        }
    }

    /// Override timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn with_client<F, Fut>(&self, f: F) -> Result<Map<String, Value>>
    where
        F: FnOnce(Client) -> Fut,
        Fut: std::future::Future<Output = Result<Map<String, Value>>>,
    {
        let client = Client::new(&self.ws_url);
        connect_with_retries(&client, 5, Duration::from_millis(250)).await?;
        let result = f(client.clone()).await;
        let _ = client.close().await;
        result
    }

    async fn rpc(&self, method: &str, params: Map<String, Value>) -> Result<Map<String, Value>> {
        let timeout = self.timeout;
        self.with_client(|c| async move { c.request(method, params, timeout).await })
            .await
    }

    /// Generic request.
    pub async fn request(
        &self,
        method: &str,
        params: Map<String, Value>,
    ) -> Result<Map<String, Value>> {
        self.rpc(method, params).await
    }

    /// `job_create`.
    pub async fn job_create(
        &self,
        goal: &str,
        workspace: Option<&str>,
    ) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("goal".into(), json!(goal));
        if let Some(ws) = workspace {
            params.insert("workspace".into(), json!(ws));
        }
        self.rpc("job_create", params).await
    }

    /// `job_status`.
    pub async fn job_status(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.rpc("job_status", params).await
    }

    /// `job_pause`.
    pub async fn job_pause(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.rpc("job_pause", params).await
    }

    /// `job_resume`.
    pub async fn job_resume(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.rpc("job_resume", params).await
    }

    /// `job_cancel`.
    pub async fn job_cancel(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.rpc("job_cancel", params).await
    }

    /// `job_dag`.
    pub async fn job_dag(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.rpc("job_dag", params).await
    }

    /// `job_guidance`.
    pub async fn job_guidance(
        &self,
        job_id: &str,
        content: &str,
        goal_id: Option<&str>,
    ) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        params.insert("content".into(), json!(content));
        if let Some(g) = goal_id {
            params.insert("goal_id".into(), json!(g));
        }
        self.rpc("job_guidance", params).await
    }

    /// `autopilot_status`.
    pub async fn autopilot_status(&self) -> Result<Map<String, Value>> {
        self.rpc("autopilot_status", Map::new()).await
    }

    /// `autopilot_submit`.
    pub async fn autopilot_submit(
        &self,
        description: &str,
        priority: i32,
        workspace: Option<&str>,
    ) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("description".into(), json!(description));
        params.insert("priority".into(), json!(priority));
        if let Some(ws) = workspace {
            params.insert("workspace".into(), json!(ws));
        }
        self.rpc("autopilot_submit", params).await
    }

    /// `autopilot_list_goals`.
    pub async fn autopilot_list_goals(&self) -> Result<Map<String, Value>> {
        self.rpc("autopilot_list_goals", Map::new()).await
    }

    /// `autopilot_get_goal`.
    pub async fn autopilot_get_goal(&self, goal_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("goal_id".into(), json!(goal_id));
        self.rpc("autopilot_get_goal", params).await
    }

    /// `autopilot_cancel_goal`.
    pub async fn autopilot_cancel_goal(&self, goal_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("goal_id".into(), json!(goal_id));
        self.rpc("autopilot_cancel_goal", params).await
    }

    /// `autopilot_cancel_all`.
    pub async fn autopilot_cancel_all(&self) -> Result<Map<String, Value>> {
        self.rpc("autopilot_cancel_all", Map::new()).await
    }

    /// `autopilot_wake`.
    pub async fn autopilot_wake(&self) -> Result<Map<String, Value>> {
        self.rpc("autopilot_wake", Map::new()).await
    }

    /// `autopilot_dream`.
    pub async fn autopilot_dream(&self) -> Result<Map<String, Value>> {
        self.rpc("autopilot_dream", Map::new()).await
    }

    /// `autopilot_resume`.
    pub async fn autopilot_resume(&self, goal_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("goal_id".into(), json!(goal_id));
        self.rpc("autopilot_resume", params).await
    }

    /// `autopilot_list_jobs`.
    pub async fn autopilot_list_jobs(&self) -> Result<Map<String, Value>> {
        self.rpc("autopilot_list_jobs", Map::new()).await
    }

    /// `autopilot_get_job`.
    pub async fn autopilot_get_job(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.rpc("autopilot_get_job", params).await
    }

    /// `cron_add` (normalized to `{"job": {...}}` when possible).
    pub async fn cron_add(&self, text: &str, priority: Option<i32>) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("text".into(), json!(text));
        if let Some(p) = priority {
            params.insert("priority".into(), json!(p));
        }
        let result = self.rpc("cron_add", params).await?;
        Ok(normalize_cron_add(result))
    }

    /// `cron_list`.
    pub async fn cron_list(&self, status: Option<&str>) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        if let Some(s) = status {
            params.insert("status".into(), json!(s));
        }
        self.rpc("cron_list", params).await
    }

    /// `cron_show`.
    pub async fn cron_show(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        let result = self.rpc("cron_show", params).await?;
        Ok(normalize_cron_show(result))
    }

    /// `cron_cancel`.
    pub async fn cron_cancel(&self, job_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("job_id".into(), json!(job_id));
        self.rpc("cron_cancel", params).await
    }

    /// `memory_stats`.
    pub async fn memory_stats(&self, mode: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("mode".into(), json!(mode));
        self.rpc("memory_stats", params).await
    }
}

/// Sync wrapper around [`AsyncCommandClient`] for scripts.
#[derive(Debug, Clone)]
pub struct CommandClient {
    inner: AsyncCommandClient,
}

impl CommandClient {
    /// Create sync command client.
    pub fn new(ws_url: impl Into<String>) -> Self {
        Self {
            inner: AsyncCommandClient::new(ws_url),
        }
    }

    /// Override timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.with_timeout(timeout);
        self
    }

    fn block<F, T>(&self, f: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(f)),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| crate::errors::Error::msg(e.to_string()))?;
                rt.block_on(f)
            }
        }
    }

    /// `job_create`.
    pub fn job_create(&self, goal: &str, workspace: Option<&str>) -> Result<Map<String, Value>> {
        self.block(self.inner.job_create(goal, workspace))
    }

    /// `job_status`.
    pub fn job_status(&self, job_id: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.job_status(job_id))
    }

    /// `job_pause`.
    pub fn job_pause(&self, job_id: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.job_pause(job_id))
    }

    /// `job_resume`.
    pub fn job_resume(&self, job_id: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.job_resume(job_id))
    }

    /// `job_cancel`.
    pub fn job_cancel(&self, job_id: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.job_cancel(job_id))
    }

    /// `job_dag`.
    pub fn job_dag(&self, job_id: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.job_dag(job_id))
    }

    /// `job_guidance`.
    pub fn job_guidance(
        &self,
        job_id: &str,
        content: &str,
        goal_id: Option<&str>,
    ) -> Result<Map<String, Value>> {
        self.block(self.inner.job_guidance(job_id, content, goal_id))
    }

    /// `autopilot_status`.
    pub fn autopilot_status(&self) -> Result<Map<String, Value>> {
        self.block(self.inner.autopilot_status())
    }

    /// `autopilot_submit`.
    pub fn autopilot_submit(
        &self,
        description: &str,
        priority: i32,
        workspace: Option<&str>,
    ) -> Result<Map<String, Value>> {
        self.block(
            self.inner
                .autopilot_submit(description, priority, workspace),
        )
    }

    /// `autopilot_list_goals`.
    pub fn autopilot_list_goals(&self) -> Result<Map<String, Value>> {
        self.block(self.inner.autopilot_list_goals())
    }

    /// `autopilot_get_goal`.
    pub fn autopilot_get_goal(&self, goal_id: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.autopilot_get_goal(goal_id))
    }

    /// `autopilot_cancel_goal`.
    pub fn autopilot_cancel_goal(&self, goal_id: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.autopilot_cancel_goal(goal_id))
    }

    /// `autopilot_cancel_all`.
    pub fn autopilot_cancel_all(&self) -> Result<Map<String, Value>> {
        self.block(self.inner.autopilot_cancel_all())
    }

    /// `autopilot_wake`.
    pub fn autopilot_wake(&self) -> Result<Map<String, Value>> {
        self.block(self.inner.autopilot_wake())
    }

    /// `autopilot_dream`.
    pub fn autopilot_dream(&self) -> Result<Map<String, Value>> {
        self.block(self.inner.autopilot_dream())
    }

    /// `autopilot_resume`.
    pub fn autopilot_resume(&self, goal_id: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.autopilot_resume(goal_id))
    }

    /// `autopilot_list_jobs`.
    pub fn autopilot_list_jobs(&self) -> Result<Map<String, Value>> {
        self.block(self.inner.autopilot_list_jobs())
    }

    /// `autopilot_get_job`.
    pub fn autopilot_get_job(&self, job_id: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.autopilot_get_job(job_id))
    }

    /// `cron_add`.
    pub fn cron_add(&self, text: &str, priority: Option<i32>) -> Result<Map<String, Value>> {
        self.block(self.inner.cron_add(text, priority))
    }

    /// `cron_list`.
    pub fn cron_list(&self, status: Option<&str>) -> Result<Map<String, Value>> {
        self.block(self.inner.cron_list(status))
    }

    /// `cron_show`.
    pub fn cron_show(&self, job_id: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.cron_show(job_id))
    }

    /// `cron_cancel`.
    pub fn cron_cancel(&self, job_id: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.cron_cancel(job_id))
    }

    /// `memory_stats`.
    pub fn memory_stats(&self, mode: &str) -> Result<Map<String, Value>> {
        self.block(self.inner.memory_stats(mode))
    }

    /// Generic request.
    pub fn request(&self, method: &str, params: Map<String, Value>) -> Result<Map<String, Value>> {
        self.block(self.inner.request(method, params))
    }
}

fn normalize_cron_add(result: Map<String, Value>) -> Map<String, Value> {
    if result.contains_key("job") {
        return result;
    }
    let job_id = result.get("job_id").or_else(|| result.get("id")).cloned();
    if let Some(id) = job_id {
        let mut job = result.clone();
        job.insert("id".into(), id);
        job.remove("job_id");
        let mut out = Map::new();
        out.insert("job".into(), Value::Object(job));
        if result.get("duplicate").and_then(|v| v.as_bool()) == Some(true) {
            out.insert("duplicate".into(), json!(true));
        }
        return out;
    }
    result
}

fn normalize_cron_show(result: Map<String, Value>) -> Map<String, Value> {
    if result.contains_key("job") {
        return result;
    }
    let job_id = result.get("job_id").or_else(|| result.get("id")).cloned();
    let Some(id) = job_id else {
        let mut out = Map::new();
        out.insert("job".into(), Value::Null);
        return out;
    };
    let mut job = result.clone();
    job.insert("id".into(), id);
    job.remove("job_id");
    let mut out = Map::new();
    out.insert("job".into(), Value::Object(job));
    out
}
