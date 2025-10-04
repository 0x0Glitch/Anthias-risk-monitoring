use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Database connection URL (PostgreSQL)
    pub database_url: String,

    /// Markets to monitor (e.g., ["LINK", "BTC", "ETH"])
    pub target_markets: Vec<String>,

    /// Monitoring interval in seconds (default: 1.0)
    #[serde(default = "default_monitoring_interval")]
    pub monitoring_interval_secs: f64,

    /// Hyperliquid API URL
    #[serde(default = "default_hyperliquid_url")]
    pub hyperliquid_api_url: String,

    /// Poll interval for Hyperliquid API in seconds (default: 1.0)
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: f64,

    /// Database connection pool settings
    #[serde(default = "default_min_connections")]
    pub min_db_connections: usize,

    #[serde(default = "default_max_connections")]
    pub max_db_connections: usize,
}

fn default_monitoring_interval() -> f64 {
    1.0
}

fn default_hyperliquid_url() -> String {
    "https://api.hyperliquid.xyz/info".to_string()
}

fn default_poll_interval() -> f64 {
    1.0
}

fn default_min_connections() -> usize {
    5
}

fn default_max_connections() -> usize {
    20
}

impl MetricsConfig {
    pub fn monitoring_interval(&self) -> Duration {
        Duration::from_secs_f64(self.monitoring_interval_secs)
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs_f64(self.poll_interval_secs)
    }

    /// Load config from environment variables
    pub fn from_env() -> Result<Self, String> {
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| "DATABASE_URL environment variable not set")?;

        let target_markets = std::env::var("TARGET_MARKETS")
            .unwrap_or_else(|_| "LINK".to_string())
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .collect();

        let monitoring_interval_secs = std::env::var("MONITORING_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(default_monitoring_interval);

        let poll_interval_secs = std::env::var("POLL_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(default_poll_interval);

        Ok(Self {
            database_url,
            target_markets,
            monitoring_interval_secs,
            hyperliquid_api_url: default_hyperliquid_url(),
            poll_interval_secs,
            min_db_connections: default_min_connections(),
            max_db_connections: default_max_connections(),
        })
    }
}
