use crate::market_metrics::types::MarketMetrics;
use deadpool_postgres::{Config, Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use log::{error, info};
use rust_decimal::Decimal;
use std::collections::HashSet;
use tokio_postgres::NoTls;

pub struct MetricsDatabase {
    pool: Pool,
    created_tables: HashSet<String>,
}

impl MetricsDatabase {
    pub async fn new(database_url: &str, max_connections: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let mut cfg = Config::new();
        cfg.url = Some(database_url.to_string());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: max_connections,
            timeouts: Default::default(),
            queue_mode: Default::default(),
        });

        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

        let db = Self {
            pool,
            created_tables: HashSet::new(),
        };

        // Create schema
        db.create_schema().await?;

        info!("Database connection pool established");
        Ok(db)
    }

    async fn create_schema(&self) -> Result<(), Box<dyn std::error::Error>> {
        let client = self.pool.get().await?;
        client
            .execute("CREATE SCHEMA IF NOT EXISTS market_metrics", &[])
            .await?;
        info!("Schema 'market_metrics' created/verified");
        Ok(())
    }

    pub async fn ensure_market_table(&mut self, coin_symbol: &str) -> Result<(), Box<dyn std::error::Error>> {
        let table_name = format!("{}_metrics_raw", coin_symbol.to_lowercase());

        if self.created_tables.contains(&table_name) {
            return Ok(());
        }

        let client = self.pool.get().await?;

        let schema_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS market_metrics.{table_name} (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                coin VARCHAR(20) NOT NULL,
                mark_price DECIMAL(20, 8),
                oracle_price DECIMAL(20, 8),
                mid_price DECIMAL(20, 8),
                best_bid DECIMAL(20, 8),
                best_ask DECIMAL(20, 8),
                spread DECIMAL(20, 8),
                spread_pct DECIMAL(10, 6),
                funding_rate_pct DECIMAL(12, 10),
                open_interest DECIMAL(20, 8),
                volume_24h DECIMAL(20, 8),
                bid_depth_5pct DECIMAL(20, 8),
                ask_depth_5pct DECIMAL(20, 8),
                total_depth_5pct DECIMAL(20, 8),
                bid_depth_10pct DECIMAL(20, 8),
                ask_depth_10pct DECIMAL(20, 8),
                total_depth_10pct DECIMAL(20, 8),
                bid_depth_25pct DECIMAL(20, 8),
                ask_depth_25pct DECIMAL(20, 8),
                total_depth_25pct DECIMAL(20, 8),
                premium DECIMAL(12, 10),
                impact_px_bid DECIMAL(20, 8),
                impact_px_ask DECIMAL(20, 8),
                node_latency_ms INTEGER,
                websocket_latency_ms INTEGER,
                total_latency_ms INTEGER,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(timestamp, coin)
            );

            CREATE INDEX IF NOT EXISTS idx_{coin_lower}_metrics_timestamp
                ON market_metrics.{table_name}(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_{coin_lower}_metrics_coin_timestamp
                ON market_metrics.{table_name}(coin, timestamp DESC);
            "#,
            table_name = table_name,
            coin_lower = coin_symbol.to_lowercase()
        );

        client.batch_execute(&schema_sql).await?;
        self.created_tables.insert(table_name.clone());
        info!("✓ Created/verified table: market_metrics.{}", table_name);

        Ok(())
    }

    pub async fn insert_metrics(&self, metrics: &MarketMetrics) -> Result<(), Box<dyn std::error::Error>> {
        let table_name = format!("{}_metrics_raw", metrics.coin.to_lowercase());
        let client = self.pool.get().await?;

        let query = format!(
            r#"
            INSERT INTO market_metrics.{} (
                coin, mark_price, oracle_price, mid_price,
                best_bid, best_ask, spread, spread_pct,
                funding_rate_pct, open_interest, volume_24h,
                bid_depth_5pct, ask_depth_5pct, total_depth_5pct,
                bid_depth_10pct, ask_depth_10pct, total_depth_10pct,
                bid_depth_25pct, ask_depth_25pct, total_depth_25pct,
                premium, impact_px_bid, impact_px_ask,
                node_latency_ms, websocket_latency_ms, total_latency_ms,
                timestamp
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27
            )
            "#,
            table_name
        );

        client
            .execute(
                &query,
                &[
                    &metrics.coin,
                    &metrics.mark_price,
                    &metrics.oracle_price,
                    &metrics.mid_price,
                    &metrics.best_bid,
                    &metrics.best_ask,
                    &metrics.spread,
                    &metrics.spread_pct,
                    &metrics.funding_rate_pct,
                    &metrics.open_interest,
                    &metrics.volume_24h,
                    &metrics.bid_depth_5pct,
                    &metrics.ask_depth_5pct,
                    &metrics.total_depth_5pct,
                    &metrics.bid_depth_10pct,
                    &metrics.ask_depth_10pct,
                    &metrics.total_depth_10pct,
                    &metrics.bid_depth_25pct,
                    &metrics.ask_depth_25pct,
                    &metrics.total_depth_25pct,
                    &metrics.premium,
                    &metrics.impact_px_bid,
                    &metrics.impact_px_ask,
                    &metrics.node_latency_ms,
                    &metrics.websocket_latency_ms,
                    &metrics.total_latency_ms,
                    &metrics.timestamp,
                ],
            )
            .await?;

        Ok(())
    }
}
