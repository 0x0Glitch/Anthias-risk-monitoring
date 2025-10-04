use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMetrics {
    pub coin: String,
    pub timestamp: DateTime<Utc>,

    // Prices from Hyperliquid API
    pub mark_price: Option<Decimal>,
    pub oracle_price: Option<Decimal>,
    pub mid_price: Option<Decimal>,

    // Order book data
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub spread: Option<Decimal>,
    pub spread_pct: Option<Decimal>,

    // Market data from Hyperliquid
    pub funding_rate_pct: Option<Decimal>,
    pub open_interest: Option<Decimal>,
    pub volume_24h: Option<Decimal>,

    // Liquidity depth from order book
    pub bid_depth_5pct: Option<Decimal>,
    pub ask_depth_5pct: Option<Decimal>,
    pub total_depth_5pct: Option<Decimal>,
    pub bid_depth_10pct: Option<Decimal>,
    pub ask_depth_10pct: Option<Decimal>,
    pub total_depth_10pct: Option<Decimal>,
    pub bid_depth_25pct: Option<Decimal>,
    pub ask_depth_25pct: Option<Decimal>,
    pub total_depth_25pct: Option<Decimal>,

    // Impact prices from Hyperliquid
    pub premium: Option<Decimal>,
    pub impact_px_bid: Option<Decimal>,
    pub impact_px_ask: Option<Decimal>,

    // Latency metrics
    pub node_latency_ms: Option<i32>,
    pub websocket_latency_ms: Option<i32>,
    pub total_latency_ms: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidMarketData {
    pub coin: String,
    pub mark_price: Decimal,
    pub oracle_price: Decimal,
    pub mid_price: Decimal,
    pub funding_rate_pct: Decimal,
    pub open_interest: Decimal,
    pub volume_24h: Decimal,
    pub premium: Decimal,
    pub impact_px_bid: Option<Decimal>,
    pub impact_px_ask: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookMetrics {
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub mid_price: Decimal,
    pub spread: Decimal,
    pub spread_pct: Decimal,
    pub total_bids: usize,
    pub total_asks: usize,
    pub bid_depth_5pct: Decimal,
    pub ask_depth_5pct: Decimal,
    pub total_depth_5pct: Decimal,
    pub bid_depth_10pct: Decimal,
    pub ask_depth_10pct: Decimal,
    pub total_depth_10pct: Decimal,
    pub bid_depth_25pct: Decimal,
    pub ask_depth_25pct: Decimal,
    pub total_depth_25pct: Decimal,
}

impl MarketMetrics {
    pub fn new(coin: String) -> Self {
        Self {
            coin,
            timestamp: Utc::now(),
            mark_price: None,
            oracle_price: None,
            mid_price: None,
            best_bid: None,
            best_ask: None,
            spread: None,
            spread_pct: None,
            funding_rate_pct: None,
            open_interest: None,
            volume_24h: None,
            bid_depth_5pct: None,
            ask_depth_5pct: None,
            total_depth_5pct: None,
            bid_depth_10pct: None,
            ask_depth_10pct: None,
            total_depth_10pct: None,
            bid_depth_25pct: None,
            ask_depth_25pct: None,
            total_depth_25pct: None,
            premium: None,
            impact_px_bid: None,
            impact_px_ask: None,
            node_latency_ms: None,
            websocket_latency_ms: None,
            total_latency_ms: None,
        }
    }

    pub fn merge_hyperliquid_data(&mut self, data: HyperliquidMarketData) {
        self.mark_price = Some(data.mark_price);
        self.oracle_price = Some(data.oracle_price);
        self.funding_rate_pct = Some(data.funding_rate_pct);
        self.open_interest = Some(data.open_interest);
        self.volume_24h = Some(data.volume_24h);
        self.premium = Some(data.premium);
        self.impact_px_bid = data.impact_px_bid;
        self.impact_px_ask = data.impact_px_ask;
    }

    pub fn merge_orderbook_data(&mut self, data: OrderBookMetrics) {
        self.best_bid = Some(data.best_bid);
        self.best_ask = Some(data.best_ask);
        self.mid_price = Some(data.mid_price);
        self.spread = Some(data.spread);
        self.spread_pct = Some(data.spread_pct);
        self.bid_depth_5pct = Some(data.bid_depth_5pct);
        self.ask_depth_5pct = Some(data.ask_depth_5pct);
        self.total_depth_5pct = Some(data.total_depth_5pct);
        self.bid_depth_10pct = Some(data.bid_depth_10pct);
        self.ask_depth_10pct = Some(data.ask_depth_10pct);
        self.total_depth_10pct = Some(data.total_depth_10pct);
        self.bid_depth_25pct = Some(data.bid_depth_25pct);
        self.ask_depth_25pct = Some(data.ask_depth_25pct);
        self.total_depth_25pct = Some(data.total_depth_25pct);
    }
}
