-- Database schema for Hyperliquid position monitoring system
-- This schema stores live position data for tracked addresses

-- Create database extension for better numeric handling
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Main table for storing live position data
CREATE TABLE IF NOT EXISTS live_positions (
    -- Primary identifiers
    address VARCHAR(42) NOT NULL,
    market VARCHAR(20) NOT NULL,
    
    -- Position details
    position_size NUMERIC(20, 8),
    entry_price NUMERIC(20, 8),
    liquidation_price NUMERIC(20, 8),
    
    -- Margin and value calculations
    margin_used NUMERIC(20, 8),
    position_value NUMERIC(20, 8),
    unrealized_pnl NUMERIC(20, 8),
    return_on_equity NUMERIC(10, 6),
    
    -- Leverage information
    leverage_type VARCHAR(10) DEFAULT 'cross',
    leverage_value INTEGER,
    leverage_raw_usd NUMERIC(20, 8),
    
    -- Account summary
    account_value NUMERIC(20, 8),
    total_margin_used NUMERIC(20, 8),
    withdrawable NUMERIC(20, 8),
    
    -- Timestamps
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key on address and market combination
    PRIMARY KEY (address, market)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_live_positions_market ON live_positions(market);
CREATE INDEX IF NOT EXISTS idx_live_positions_value ON live_positions(position_value);
CREATE INDEX IF NOT EXISTS idx_live_positions_updated ON live_positions(last_updated);

-- Create a function to update the last_updated timestamp
CREATE OR REPLACE FUNCTION update_last_updated_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update last_updated on row changes
CREATE TRIGGER update_live_positions_last_updated 
    BEFORE UPDATE ON live_positions 
    FOR EACH ROW 
    EXECUTE FUNCTION update_last_updated_column();
