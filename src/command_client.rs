//! Ephemeral command clients for jobs / cron one-shots.

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

    /// `job_create`. `rail_id` is required (loop-native submission path).
    pub async fn job_create(
        &self,
        goal: &str,
        rail_id: &str,
        workspace: Option<&str>,
    ) -> Result<Map<String, Value>> {
        if goal.trim().is_empty() {
            return Err(crate::errors::Error::msg("goal is required"));
        }
        if rail_id.trim().is_empty() {
            return Err(crate::errors::Error::msg("rail_id is required"));
        }
        let mut params = Map::new();
        params.insert("goal".into(), json!(goal));
        params.insert("rail_id".into(), json!(rail_id));
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

    /// `job_create`. `rail_id` is required (loop-native submission path).
    pub fn job_create(
        &self,
        goal: &str,
        rail_id: &str,
        workspace: Option<&str>,
    ) -> Result<Map<String, Value>> {
        self.block(self.inner.job_create(goal, rail_id, workspace))
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
