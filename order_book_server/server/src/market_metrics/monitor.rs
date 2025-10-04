use crate::listeners::order_book::OrderBookListener;
use crate::market_metrics::{
    HyperliquidClient, MetricsConfig, MetricsDatabase, MarketMetrics,
    types::OrderBookMetrics,
};
use crate::order_book::Coin;
use chrono::Utc;
use log::{error, info, warn};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

pub struct MarketMetricsMonitor {
    config: MetricsConfig,
    database: Arc<Mutex<MetricsDatabase>>,
    hyperliquid_client: Arc<HyperliquidClient>,
    orderbook_listener: Arc<Mutex<OrderBookListener>>,
}

impl MarketMetricsMonitor {
    pub async fn new(
        config: MetricsConfig,
        orderbook_listener: Arc<Mutex<OrderBookListener>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Create database connection
        let mut database = MetricsDatabase::new(
            &config.database_url,
            config.max_db_connections,
        )
        .await?;

        // Ensure tables exist for all target markets
        for market in &config.target_markets {
            database.ensure_market_table(market).await?;
        }

        let database = Arc::new(Mutex::new(database));

        // Create Hyperliquid client
        let hyperliquid_client = Arc::new(HyperliquidClient::new(
            config.hyperliquid_api_url.clone(),
            config.poll_interval(),
        ));

        // Start background polling for Hyperliquid data
        hyperliquid_client.clone().start_polling();

        info!(" Market metrics monitor initialized");
        info!("  - Target markets: {:?}", config.target_markets);
        info!("  - Monitoring interval: {:?}", config.monitoring_interval());
        info!("  - Poll interval: {:?}", config.poll_interval());

        Ok(Self {
            config,
            database,
            hyperliquid_client,
            orderbook_listener,
        })
    }

    /// Start monitoring all configured markets
    pub async fn start(self: Arc<Self>) {
        info!("🎯 Starting market metrics monitoring");

        // Spawn a monitoring task for each market
        for market in &self.config.target_markets {
            let monitor = self.clone();
            let market = market.clone();
            tokio::spawn(async move {
                monitor.monitor_market(market).await;
            });
        }

        info!("✅ All market monitoring tasks started");
    }

    /// Monitor a single market continuously
    async fn monitor_market(&self, market: String) {
        let mut interval = interval(self.config.monitoring_interval());
        info!("📊 Started monitoring {}", market);

        loop {
            interval.tick().await;

            match self.collect_and_store_metrics(&market).await {
                Ok(_) => {}
                Err(e) => {
                    error!("Failed to collect metrics for {}: {}", market, e);
                }
            }
        }
    }

    /// Collect metrics for a market and store in database
    async fn collect_and_store_metrics(&self, coin: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut metrics = MarketMetrics::new(coin.to_string());
        metrics.timestamp = Utc::now();

        // Get Hyperliquid market data
        if let Some(hl_data) = self.hyperliquid_client.get_market_data(coin).await {
            metrics.merge_hyperliquid_data(hl_data);
        } else {
            warn!("{}: No Hyperliquid data available", coin);
        }

        // Get orderbook metrics
        if let Some(ob_metrics) = self.get_orderbook_metrics(coin).await {
            metrics.merge_orderbook_data(ob_metrics);
        } else {
            warn!("{}: No orderbook data available", coin);
        }

        // Insert into database
        let db = self.database.lock().await;
        db.insert_metrics(&metrics).await?;

        let price = metrics.mark_price.unwrap_or_default();
        info!("📊 {}: ${} - metrics inserted ✅", coin, price);

        Ok(())
    }

    /// Extract orderbook metrics from the listener
    async fn get_orderbook_metrics(&self, coin: &str) -> Option<OrderBookMetrics> {
        let mut listener = self.orderbook_listener.lock().await;

        // Get snapshot from listener
        let snapshot = listener.compute_snapshot()?;
        let coin_obj = Coin::new(coin);

        // Find the snapshot for this coin and store the value to extend its lifetime
        let snapshot_value = snapshot.snapshot.value();
        let (_, snapshot_data) = snapshot_value
            .iter()
            .find(|(c, _)| **c == coin_obj)?;

        // Parse bids and asks
        let bids = &snapshot_data.as_ref()[0];
        let asks = &snapshot_data.as_ref()[1];

        if bids.is_empty() || asks.is_empty() {
            return None;
        }

        // Calculate best prices (limit_px and sz are Px/Sz types with to_str() method)
        let best_bid = Decimal::from_str(&bids[0].limit_px.to_str()).ok()?;
        let best_ask = Decimal::from_str(&asks[0].limit_px.to_str()).ok()?;
        let mid_price = (best_bid + best_ask) / Decimal::from(2);

        // Calculate spread
        let spread = best_ask - best_bid;
        let spread_pct = (spread / mid_price) * Decimal::from(100);

        // Calculate depth at various levels (convert Px/Sz to Decimal via to_str())
        let bid_levels = bids
            .iter()
            .filter_map(|order| {
                Some((
                    Decimal::from_str(&order.limit_px.to_str()).ok()?,
                    Decimal::from_str(&order.sz.to_str()).ok()?,
                ))
            })
            .collect::<Vec<_>>();

        let ask_levels = asks
            .iter()
            .filter_map(|order| {
                Some((
                    Decimal::from_str(&order.limit_px.to_str()).ok()?,
                    Decimal::from_str(&order.sz.to_str()).ok()?,
                ))
            })
            .collect::<Vec<_>>();

        let depths = calculate_liquidity_depth(&bid_levels, &ask_levels, mid_price);

        Some(OrderBookMetrics {
            best_bid,
            best_ask,
            mid_price,
            spread,
            spread_pct,
            total_bids: bids.len(),
            total_asks: asks.len(),
            bid_depth_5pct: depths.0,
            ask_depth_5pct: depths.1,
            total_depth_5pct: depths.0 + depths.1,
            bid_depth_10pct: depths.2,
            ask_depth_10pct: depths.3,
            total_depth_10pct: depths.2 + depths.3,
            bid_depth_25pct: depths.4,
            ask_depth_25pct: depths.5,
            total_depth_25pct: depths.4 + depths.5,
        })
    }
}

/// Calculate liquidity depth at 5%, 10%, and 25% levels
fn calculate_liquidity_depth(
    bids: &[(Decimal, Decimal)],
    asks: &[(Decimal, Decimal)],
    mid_price: Decimal,
) -> (Decimal, Decimal, Decimal, Decimal, Decimal, Decimal) {
    let percentages = [
        Decimal::from_str("0.05").unwrap(),
        Decimal::from_str("0.10").unwrap(),
        Decimal::from_str("0.25").unwrap(),
    ];

    let mut results = vec![];

    for pct in &percentages {
        let bid_threshold = mid_price * (Decimal::ONE - pct);
        let ask_threshold = mid_price * (Decimal::ONE + pct);

        let bid_depth: Decimal = bids
            .iter()
            .filter(|(price, _)| *price >= bid_threshold)
            .map(|(price, size)| price * size)
            .sum();

        let ask_depth: Decimal = asks
            .iter()
            .filter(|(price, _)| *price <= ask_threshold)
            .map(|(price, size)| price * size)
            .sum();

        results.push((bid_depth, ask_depth));
    }

    (
        results[0].0,
        results[0].1,
        results[1].0,
        results[1].1,
        results[2].0,
        results[2].1,
    )
}
