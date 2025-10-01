#!/bin/bash
# Startup script for Hyperliquid Position Monitor

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Hyperliquid Position Monitor - Production Startup${NC}"
echo "=========================================="

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo -e "${RED}Error: uv is not installed. Install it with:${NC}"
    echo "curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(uv run python --version 2>&1 | grep -Po '(?<=Python )\d+\.\d+')
REQUIRED_VERSION="3.8"
if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then 
    echo -e "${RED}Error: Python $REQUIRED_VERSION or higher is required (found $PYTHON_VERSION)${NC}"
    exit 1
fi

# Install/sync dependencies with uv
echo -e "${YELLOW}Syncing dependencies with uv...${NC}"
uv sync

# Check environment file
if [ ! -f ".env" ]; then
    echo -e "${RED}Error: .env file not found!${NC}"
    echo "Please copy .env.example to .env and configure it:"
    echo "  cp .env.example .env"
    exit 1
fi

# Load environment variables
set -a
source .env
set +a

# Validate critical environment variables
if [ -z "$DATABASE_URL" ]; then
    echo -e "${RED}Error: DATABASE_URL not set in .env${NC}"
    exit 1
fi

# Create data directory if it doesn't exist
DATA_DIR="${DATA_DIR:-./data}"
mkdir -p "$DATA_DIR"
echo "Data directory: $DATA_DIR"

# Check if node binary exists
NODE_BINARY_PATH="${NODE_BINARY_PATH:-~/hl-node}"
NODE_BINARY_EXPANDED=$(eval echo "$NODE_BINARY_PATH")
if [ ! -f "$NODE_BINARY_EXPANDED" ]; then
    echo -e "${YELLOW}Warning: hl-node binary not found at $NODE_BINARY_EXPANDED${NC}"
    echo "RMP snapshot processing will not work without it"
    echo "Download from: https://github.com/hyperliquid-dex/node"
fi

# Check if RMP directory exists
RMP_BASE_PATH="${RMP_BASE_PATH:-~/hl/data/periodic_abci_states}"
RMP_BASE_EXPANDED=$(eval echo "$RMP_BASE_PATH")
if [ ! -d "$RMP_BASE_EXPANDED" ]; then
    echo -e "${YELLOW}Warning: RMP directory not found at $RMP_BASE_EXPANDED${NC}"
    echo "Make sure your Hyperliquid node is running and generating snapshots"
    mkdir -p "$RMP_BASE_EXPANDED"
fi

# Check database connection
echo "Testing database connection..."
uv run python -c "
import asyncio
import asyncpg
import os

async def test():
    try:
        conn = await asyncpg.connect(
            os.environ['DATABASE_URL'],
            statement_cache_size=0,
            server_settings={'jit': 'off'}
        )
        version = await conn.fetchval('SELECT version()')
        await conn.close()
        print('✅ Database connection successful')
        print(f'   PostgreSQL: {version.split()[0]} {version.split()[1]}')
    except Exception as e:
        print(f'❌ Database connection failed: {e}')
        exit(1)

asyncio.run(test())
" || exit 1

# Check local API if configured
NVN_API_URL="${NVN_API_URL:-http://127.0.0.1:3001/info}"
echo "Testing local node API at $NVN_API_URL..."
if curl -s -X POST "$NVN_API_URL" \
    -H "Content-Type: application/json" \
    -d '{"type":"exchangeStatus"}' \
    --max-time 5 > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Local node API is responsive${NC}"
else
    echo -e "${YELLOW}⚠️  Local node API not responding${NC}"
    echo "The monitor will use the public API as fallback"
fi

# Display configuration
echo ""
echo -e "${GREEN}Configuration:${NC}"
echo "  Target Markets: ${TARGET_MARKETS:-BTC,ETH,LINK}"
echo "  Min Position Size: \$${MIN_POSITION_SIZE_USD:-10000}"
echo "  Snapshot Check: Every ${SNAPSHOT_CHECK_INTERVAL:-60} seconds"
echo "  Position Refresh: Every ${POSITION_REFRESH_INTERVAL:-10} seconds"
echo "  Data Directory: $DATA_DIR"
echo ""

# Start the monitor
echo -e "${GREEN}Starting Hyperliquid Position Monitor...${NC}"
echo "=========================================="
echo "Press Ctrl+C to stop"
echo ""

# Run with uv for proper dependency management
uv run python -u monitor.py
