-- Database schema for Hyperliquid Position Monitoring System
-- Table: live_positions
-- Purpose: Store real-time position data for monitored addresses

CREATE TABLE IF NOT EXISTS live_positions (
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
    CONSTRAINT live_positions_pkey PRIMARY KEY (address, market)
);

-- Indexes for efficient queries

-- Index for filtering by market
CREATE INDEX IF NOT EXISTS idx_live_positions_market 
    ON live_positions USING btree (market);

-- Index for filtering by last update time
CREATE INDEX IF NOT EXISTS idx_live_positions_last_updated 
    ON live_positions USING btree (last_updated);

-- Index for filtering by position value
CREATE INDEX IF NOT EXISTS idx_live_positions_position_value 
    ON live_positions USING btree (position_value);

-- Index for filtering by address
CREATE INDEX IF NOT EXISTS idx_live_positions_address 
    ON live_positions USING btree (address);

-- Partial index for high-value positions (commonly filtered threshold)
CREATE INDEX IF NOT EXISTS idx_live_positions_value_threshold 
    ON live_positions (market, position_value) 
    WHERE position_value >= 10000;

-- Comments for documentation
COMMENT ON TABLE live_positions IS 'Real-time position data for monitored Hyperliquid addresses';
COMMENT ON COLUMN live_positions.address IS 'Ethereum address of the trader (lowercase)';
COMMENT ON COLUMN live_positions.market IS 'Trading market symbol (uppercase, e.g., BTC, ETH, LINK)';
COMMENT ON COLUMN live_positions.position_size IS 'Position size (positive for long, negative for short)';
COMMENT ON COLUMN live_positions.leverage_type IS 'Leverage type: cross or isolated';
