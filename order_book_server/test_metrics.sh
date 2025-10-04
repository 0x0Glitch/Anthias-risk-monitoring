#!/bin/bash
# Test script for market metrics monitoring

set -e

echo "========================================="
echo "Order Book Server - Metrics Test Script"
echo "========================================="
echo ""

# Check if binary exists
if [ ! -f "target/release/websocket_server" ]; then
    echo "❌ Binary not found. Building..."
    cargo build --release --bin websocket_server
fi

echo "✅ Binary found at target/release/websocket_server"
echo ""

# Check environment variables
echo "Checking environment variables..."
echo ""

if [ -z "$DATABASE_URL" ]; then
    echo "⚠️  DATABASE_URL not set"
    echo "   Example: export DATABASE_URL='postgresql://postgres:password@localhost:5432/market_data'"
else
    echo "✅ DATABASE_URL: $DATABASE_URL"
fi

if [ -z "$TARGET_MARKETS" ]; then
    echo "⚠️  TARGET_MARKETS not set, using default: LINK"
    export TARGET_MARKETS="LINK"
else
    echo "✅ TARGET_MARKETS: $TARGET_MARKETS"
fi

if [ -z "$MONITORING_INTERVAL" ]; then
    echo "ℹ️  MONITORING_INTERVAL not set, using default: 1.0"
else
    echo "✅ MONITORING_INTERVAL: $MONITORING_INTERVAL"
fi

if [ -z "$POLL_INTERVAL" ]; then
    echo "ℹ️  POLL_INTERVAL not set, using default: 1.0"
else
    echo "✅ POLL_INTERVAL: $POLL_INTERVAL"
fi

echo ""
echo "========================================="
echo ""

# Check if database is accessible
if [ -n "$DATABASE_URL" ]; then
    echo "Testing database connection..."
    if command -v psql &> /dev/null; then
        if psql "$DATABASE_URL" -c "SELECT 1;" &> /dev/null; then
            echo "✅ Database connection successful"
        else
            echo "❌ Database connection failed"
            echo "   Make sure PostgreSQL is running and DATABASE_URL is correct"
        fi
    else
        echo "⚠️  psql not found, skipping database check"
    fi
    echo ""
fi

echo "========================================="
echo "Starting server with metrics monitoring"
echo "========================================="
echo ""
echo "Address: 0.0.0.0:8000"
echo "Metrics: ENABLED"
echo ""
echo "Press Ctrl+C to stop"
echo ""

# Run the server
export RUST_LOG=info

./target/release/websocket_server \
    --address 0.0.0.0 \
    --port 8000 \
    --enable-metrics
