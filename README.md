# Hyperliquid Position Monitoring System

A production-ready monitoring system for tracking user positions on Hyperliquid perpetual markets using RMP snapshots and clearinghouseState API.

## Features

- **RMP-based User Discovery**: Processes periodic L1 snapshots to discover users with positions
- **Real-time Position Updates**: Continuous monitoring via clearinghouseState API
- **Batch Processing**: Efficient concurrent processing of hundreds of addresses
- **Dual API Fallback**: Automatic failover from local node to public API
- **Database Integration**: PostgreSQL storage with optimized schema
- **Health Monitoring**: Component health tracking and automatic recovery
- **Production Logging**: Structured logging with rotation and multiple streams

## Quick Start

1. **Install dependencies**:
```bash
uv sync
```

2. **Configure environment**:
```bash
cp .env.example .env
# Edit .env with your settings
```

3. **Run the monitor**:
```bash
./run.sh
```

## Configuration

### Required Environment Variables

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/hyperliquid
TARGET_MARKETS=BTC,ETH,LINK
MIN_POSITION_SIZE_USD=10000
RMP_BASE_PATH=~/hl/data/periodic_abci_states
NODE_BINARY_PATH=~/hl-node
```

## Architecture

The system implements a robust monitoring pipeline:

```
RMP Snapshots → User Discovery → Address Management → Position Updates → Database Storage
```

### Core Components

- **SnapshotProcessor**: Processes RMP snapshots for user discovery
- **AddressManager**: Manages active addresses with file persistence  
- **PositionUpdater**: Updates positions via clearinghouseState API
- **DatabaseManager**: Handles PostgreSQL operations and connection pooling

## Database Schema

```sql
CREATE TABLE live_positions (
    address VARCHAR(42) NOT NULL,
    market VARCHAR(20) NOT NULL,
    position_size NUMERIC(20, 8),
    entry_price NUMERIC(20, 8),
    liquidation_price NUMERIC(20, 8),
    margin_used NUMERIC(20, 8),
    position_value NUMERIC(20, 8),
    unrealized_pnl NUMERIC(20, 8),
    return_on_equity NUMERIC(10, 6),
    leverage_type VARCHAR(10) DEFAULT 'cross',
    leverage_value INTEGER,
    leverage_raw_usd NUMERIC(20, 8),
    account_value NUMERIC(20, 8),
    total_margin_used NUMERIC(20, 8),
    withdrawable NUMERIC(20, 8),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (address, market)
);
```

## License

MIT