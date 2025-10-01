-- =========================================================================
-- Database Schema for Hyperliquid Position Monitoring System
-- =========================================================================
-- Creates a dedicated user_metrics schema with token-specific tables
-- Each trading pair (BTC, ETH, LINK, etc.) has its own positions table

-- Create user_metrics schema
CREATE SCHEMA IF NOT EXISTS user_metrics;

-- =========================================================================
-- UTILITY FUNCTIONS
-- =========================================================================

-- Function to create a position table for a specific token
CREATE OR REPLACE FUNCTION user_metrics.create_token_positions_table(token_name TEXT)
RETURNS VOID AS $$
DECLARE
    table_name TEXT;
BEGIN
    -- Validate token name (only alphanumeric and underscore)
    IF token_name !~ '^[a-zA-Z0-9_]+$' THEN
        RAISE EXCEPTION 'Invalid token name: %', token_name;
    END IF;
    
    table_name := token_name || '_live_positions';
    
    -- Create the positions table
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS user_metrics.%I (
            -- Primary identifiers
            address VARCHAR(42) NOT NULL,
            market VARCHAR(20) NOT NULL,
            
            -- Position details
            position_size NUMERIC(20, 8) NULL DEFAULT 0,
            entry_price NUMERIC(20, 8) NULL,
            liquidation_price NUMERIC(20, 8) NULL,
            margin_used NUMERIC(20, 8) NULL,
            position_value NUMERIC(20, 8) NULL,
            unrealized_pnl NUMERIC(20, 8) NULL,
            return_on_equity NUMERIC(10, 6) NULL,
            
            -- Leverage information
            leverage_type VARCHAR(10) NULL DEFAULT 'cross',
            leverage_value INTEGER NULL,
            leverage_raw_usd NUMERIC(20, 8) NULL,
            
            -- Account information
            account_value NUMERIC(20, 8) NULL,
            total_margin_used NUMERIC(20, 8) NULL,
            withdrawable NUMERIC(20, 8) NULL,
            
            -- Timestamps
            last_updated TIMESTAMP WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
            
            -- Primary key constraint
            CONSTRAINT %I PRIMARY KEY (address, market)
        );
    ', table_name, table_name || '_pkey');
    
    -- Create indexes
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_market ON user_metrics.%I USING btree (market);', table_name, table_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_last_updated ON user_metrics.%I USING btree (last_updated);', table_name, table_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_position_value ON user_metrics.%I USING btree (position_value);', table_name, table_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_address ON user_metrics.%I USING btree (address);', table_name, table_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_value_threshold ON user_metrics.%I (market, position_value) WHERE position_value >= 10000;', table_name, table_name);
    
    -- Add comments
    EXECUTE format('COMMENT ON TABLE user_metrics.%I IS ''Real-time position data for %s trading pairs'';', table_name, token_name);
    EXECUTE format('COMMENT ON COLUMN user_metrics.%I.address IS ''Ethereum address of the trader (lowercase)'';', table_name);
    EXECUTE format('COMMENT ON COLUMN user_metrics.%I.market IS ''Trading market symbol (uppercase, e.g., BTC, ETH, LINK)'';', table_name);
    EXECUTE format('COMMENT ON COLUMN user_metrics.%I.position_size IS ''Position size (positive for long, negative for short)'';', table_name);
    EXECUTE format('COMMENT ON COLUMN user_metrics.%I.leverage_type IS ''Leverage type: cross or isolated'';', table_name);
END;
$$ LANGUAGE plpgsql;

-- =========================================================================
-- SQL QUERIES CATALOG
-- Named queries extracted from core logic for better organization
-- =========================================================================

-- Query: upsert_positions
CREATE OR REPLACE FUNCTION user_metrics.get_upsert_positions_query()
RETURNS TEXT AS $$
BEGIN
    RETURN '
        INSERT INTO user_metrics.{token}_live_positions (
            address, market, position_size, entry_price, liquidation_price,
            margin_used, position_value, unrealized_pnl, return_on_equity,
            leverage_type, leverage_value, leverage_raw_usd, account_value,
            total_margin_used, withdrawable, last_updated
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW())
        ON CONFLICT (address, market)
        DO UPDATE SET
            position_size = EXCLUDED.position_size,
            entry_price = EXCLUDED.entry_price,
            liquidation_price = EXCLUDED.liquidation_price,
            margin_used = EXCLUDED.margin_used,
            position_value = EXCLUDED.position_value,
            unrealized_pnl = EXCLUDED.unrealized_pnl,
            return_on_equity = EXCLUDED.return_on_equity,
            leverage_type = EXCLUDED.leverage_type,
            leverage_value = EXCLUDED.leverage_value,
            leverage_raw_usd = EXCLUDED.leverage_raw_usd,
            account_value = EXCLUDED.account_value,
            total_margin_used = EXCLUDED.total_margin_used,
            withdrawable = EXCLUDED.withdrawable,
            last_updated = NOW()
    ';
END;
$$ LANGUAGE plpgsql;

-- Query: delete_positions
CREATE OR REPLACE FUNCTION user_metrics.get_delete_positions_query()
RETURNS TEXT AS $$
BEGIN
    RETURN 'DELETE FROM user_metrics.{token}_live_positions WHERE address = $1 AND market = $2';
END;
$$ LANGUAGE plpgsql;

-- Query: get_addresses_by_market
CREATE OR REPLACE FUNCTION user_metrics.get_addresses_by_market_query()
RETURNS TEXT AS $$
BEGIN
    RETURN '
        SELECT DISTINCT market, address 
        FROM user_metrics.{token}_live_positions 
        WHERE position_value >= $1
        ORDER BY market, address
    ';
END;
$$ LANGUAGE plpgsql;

-- Query: get_positions_filtered
CREATE OR REPLACE FUNCTION user_metrics.get_positions_filtered_query()
RETURNS TEXT AS $$
BEGIN
    RETURN '
        SELECT * FROM user_metrics.{token}_live_positions
        {where_clause}
        ORDER BY position_value DESC
    ';
END;
$$ LANGUAGE plpgsql;

-- Query: get_overall_stats
CREATE OR REPLACE FUNCTION user_metrics.get_overall_stats_query()
RETURNS TEXT AS $$
BEGIN
    RETURN '
        SELECT 
            COUNT(DISTINCT address) as unique_addresses,
            COUNT(*) as total_positions,
            SUM(position_value) as total_value_usd,
            AVG(position_value) as avg_position_value,
            MAX(position_value) as max_position_value,
            MIN(last_updated) as oldest_update,
            MAX(last_updated) as newest_update
        FROM user_metrics.{token}_live_positions
        WHERE position_value >= $1
    ';
END;
$$ LANGUAGE plpgsql;

-- Query: get_market_stats
CREATE OR REPLACE FUNCTION user_metrics.get_market_stats_query()
RETURNS TEXT AS $$
BEGIN
    RETURN '
        SELECT 
            market,
            COUNT(DISTINCT address) as addresses,
            COUNT(*) as positions,
            SUM(position_value) as total_value,
            AVG(position_value) as avg_value
        FROM user_metrics.{token}_live_positions
        WHERE position_value >= $1
        GROUP BY market
    ';
END;
$$ LANGUAGE plpgsql;

-- Query: cleanup_closed_positions
CREATE OR REPLACE FUNCTION user_metrics.get_cleanup_closed_positions_query()
RETURNS TEXT AS $$
BEGIN
    RETURN '
        DELETE FROM user_metrics.{token}_live_positions
        WHERE (position_size = 0 OR position_value = 0)
        AND last_updated < NOW() - INTERVAL ''{max_age_hours} hours''
        RETURNING address, market
    ';
END;
$$ LANGUAGE plpgsql;

-- Query: cleanup_stale_positions
CREATE OR REPLACE FUNCTION user_metrics.get_cleanup_stale_positions_query()
RETURNS TEXT AS $$
BEGIN
    RETURN '
        DELETE FROM user_metrics.{token}_live_positions
        WHERE last_updated < NOW() - INTERVAL ''{max_age_hours} hours''
        AND (position_size = 0 OR position_value = 0)
        RETURNING address, market
    ';
END;
$$ LANGUAGE plpgsql;

-- Query: bulk_delete_addresses
CREATE OR REPLACE FUNCTION user_metrics.get_bulk_delete_addresses_query()
RETURNS TEXT AS $$
BEGIN
    RETURN 'DELETE FROM user_metrics.{token}_live_positions WHERE address = ANY($1)';
END;
$$ LANGUAGE plpgsql;

-- =========================================================================
-- CREATE DEFAULT TOKEN TABLES
-- =========================================================================
-- Create tables for common trading pairs

-- Bitcoin
SELECT user_metrics.create_token_positions_table('btc');

-- Ethereum  
SELECT user_metrics.create_token_positions_table('eth');

-- Chainlink
SELECT user_metrics.create_token_positions_table('link');

-- Additional tokens can be added as needed:
-- SELECT user_metrics.create_token_positions_table('sol');
-- SELECT user_metrics.create_token_positions_table('avax');
-- SELECT user_metrics.create_token_positions_table('arb');
