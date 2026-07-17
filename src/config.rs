//! Client configuration and environment loading.

use std::time::Duration;

/// Runtime client configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Daemon WebSocket URL.
    pub daemon_url: String,
    /// Verbosity for loop_events subscribe.
    pub verbosity: String,
    /// Max connect retries for helpers.
    pub max_retries: u32,
    /// Daemon ready timeout.
    pub daemon_ready_timeout: Duration,
    /// Loop status / list timeout.
    pub loop_status_timeout: Duration,
    /// Subscription timeout.
    pub subscription_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            daemon_url: "ws://127.0.0.1:8765".into(),
            verbosity: String::new(),
            max_retries: 40,
            daemon_ready_timeout: Duration::from_secs(20),
            loop_status_timeout: Duration::from_secs(30),
            subscription_timeout: Duration::from_secs(30),
        }
    }
}

/// Load config from `SOOTHE_*` environment variables.
pub fn load_config_from_env() -> Config {
    let mut cfg = Config::default();
    if let Ok(url) = std::env::var("SOOTHE_WS_URL") {
        if !url.trim().is_empty() {
            cfg.daemon_url = url.trim().to_string();
        }
    } else if let Ok(url) = std::env::var("SOOTHE_DAEMON_URL") {
        if !url.trim().is_empty() {
            cfg.daemon_url = url.trim().to_string();
        }
    }
    if let Ok(v) = std::env::var("SOOTHE_VERBOSITY") {
        cfg.verbosity = v;
    }
    if let Ok(v) = std::env::var("SOOTHE_MAX_RETRIES") {
        if let Ok(n) = v.parse() {
            cfg.max_retries = n;
        }
    }
    if let Ok(v) = std::env::var("SOOTHE_DAEMON_READY_TIMEOUT_SEC") {
        if let Ok(n) = v.parse::<u64>() {
            cfg.daemon_ready_timeout = Duration::from_secs(n);
        }
    }
    if let Ok(v) = std::env::var("SOOTHE_LOOP_STATUS_TIMEOUT_SEC") {
        if let Ok(n) = v.parse::<u64>() {
            cfg.loop_status_timeout = Duration::from_secs(n);
        }
    }
    if let Ok(v) = std::env::var("SOOTHE_SUBSCRIPTION_TIMEOUT_SEC") {
        if let Ok(n) = v.parse::<u64>() {
            cfg.subscription_timeout = Duration::from_secs(n);
        }
    }
    cfg
}
