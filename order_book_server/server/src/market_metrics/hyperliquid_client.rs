use crate::market_metrics::types::HyperliquidMarketData;
use log::{error, info};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;

#[derive(Debug, Serialize)]
struct MetaRequest {
    #[serde(rename = "type")]
    request_type: String,
}

#[derive(Debug, Clone, Deserialize)]
struct AssetMeta {
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssetContext {
    mark_px: String,
    oracle_px: String,
    mid_px: Option<String>,
    funding: String,
    open_interest: String,
    day_ntl_vlm: String,
    premium: Option<String>,
    impact_pxs: Option<Vec<String>>,
}

pub struct HyperliquidClient {
    client: Client,
    api_url: String,
    cached_data: Arc<RwLock<HashMap<String, HyperliquidMarketData>>>,
    poll_interval: Duration,
}

impl HyperliquidClient {
    pub fn new(api_url: String, poll_interval: Duration) -> Self {
        Self {
            client: Client::new(),
            api_url,
            cached_data: Arc::new(RwLock::new(HashMap::new())),
            poll_interval,
        }
    }

    /// Start background polling task
    pub fn start_polling(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = time::interval(self.poll_interval);
            loop {
                interval.tick().await;
                if let Err(e) = self.fetch_and_cache_all_markets().await {
                    error!("Failed to fetch market data: {}", e);
                }
            }
        });
    }

    /// Fetch and cache all market data from Hyperliquid API
    async fn fetch_and_cache_all_markets(&self) -> Result<(), Box<dyn std::error::Error>> {
        let request = MetaRequest {
            request_type: "metaAndAssetCtxs".to_string(),
        };

        let response = self
            .client
            .post(&self.api_url)
            .json(&request)
            .timeout(Duration::from_secs(5))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(format!("API error: {}", response.status()).into());
        }

        let data: serde_json::Value = response.json().await?;

        // Parse response: [universe_obj, asset_ctxs]
        let array = data
            .as_array()
            .ok_or("Expected array response")?;

        if array.len() != 2 {
            return Err("Expected 2 elements in response".into());
        }

        // Extract universe
        let universe_obj = &array[0];
        let universe = if let Some(u) = universe_obj.get("universe") {
            u.as_array().ok_or("Expected universe array")?
        } else {
            universe_obj.as_array().ok_or("Expected universe array")?
        };

        let asset_ctxs = array[1]
            .as_array()
            .ok_or("Expected asset_ctxs array")?;

        // Parse into structured data
        let mut market_data_map = HashMap::new();

        for (i, meta_val) in universe.iter().enumerate() {
            if i >= asset_ctxs.len() {
                break;
            }

            let meta: AssetMeta = serde_json::from_value(meta_val.clone())?;
            let ctx: AssetContext = serde_json::from_value(asset_ctxs[i].clone())?;

            let market_data = HyperliquidMarketData {
                coin: meta.name.clone(),
                mark_price: Decimal::from_str(&ctx.mark_px).unwrap_or_default(),
                oracle_price: Decimal::from_str(&ctx.oracle_px).unwrap_or_default(),
                mid_price: ctx
                    .mid_px
                    .and_then(|s| Decimal::from_str(&s).ok())
                    .unwrap_or_default(),
                funding_rate_pct: Decimal::from_str(&ctx.funding).unwrap_or_default() * Decimal::from(100),
                open_interest: Decimal::from_str(&ctx.open_interest).unwrap_or_default()
                    * Decimal::from_str(&ctx.mark_px).unwrap_or_default(),
                volume_24h: Decimal::from_str(&ctx.day_ntl_vlm).unwrap_or_default(),
                premium: ctx
                    .premium
                    .and_then(|s| Decimal::from_str(&s).ok())
                    .unwrap_or_default(),
                impact_px_bid: ctx
                    .impact_pxs
                    .as_ref()
                    .and_then(|v| v.get(0))
                    .and_then(|s| Decimal::from_str(s).ok()),
                impact_px_ask: ctx
                    .impact_pxs
                    .as_ref()
                    .and_then(|v| v.get(1))
                    .and_then(|s| Decimal::from_str(s).ok()),
            };

            market_data_map.insert(meta.name, market_data);
        }

        // Update cache
        let mut cache = self.cached_data.write().await;
        *cache = market_data_map;
        info!("Updated market data cache: {} markets", cache.len());

        Ok(())
    }

    /// Get cached market data for a specific coin
    pub async fn get_market_data(&self, coin: &str) -> Option<HyperliquidMarketData> {
        let cache = self.cached_data.read().await;
        cache.get(coin).cloned()
    }

    /// Get fresh market data by fetching immediately
    pub async fn get_fresh_market_data(&self, coin: &str) -> Result<HyperliquidMarketData, Box<dyn std::error::Error>> {
        self.fetch_and_cache_all_markets().await?;
        self.get_market_data(coin)
            .await
            .ok_or_else(|| format!("Coin {} not found in market data", coin).into())
    }
}
