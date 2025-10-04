#!/bin/bash

# AWS Setup Script for Hyperliquid Order Book Server
# Run this script on a fresh Ubuntu 22.04 LTS instance

set -e  # Exit on any error

echo "🚀 Setting up Hyperliquid Order Book Server on AWS..."

# Update system
echo "📦 Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install build dependencies
echo "🔧 Installing build dependencies..."
sudo apt install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    cmake \
    curl \
    git \
    htop

# Install Rust
echo "🦀 Installing Rust..."
if ! command -v rustc &> /dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source ~/.bashrc
    export PATH="$HOME/.cargo/bin:$PATH"
else
    echo "✅ Rust is already installed"
fi

# Verify Rust installation
rustc --version
cargo --version

# Database configuration
echo "🔐 Setting up database connection..."
echo "Please enter your external PostgreSQL connection string:"
echo "Format: postgresql://username:password@host:port/database_name"
read -r DATABASE_URL

# Create data directories
echo "📁 Creating data directories..."
sudo mkdir -p /opt/data/hyperliquid/{snapshots,order_diffs,order_status}
sudo chown -R $USER:$USER /opt/data/hyperliquid

# Clone repository (if not already cloned)
if [ ! -d "order_book_server" ]; then
    echo "📥 Cloning repository..."
    git clone https://github.com/0x0Glitch/testing.git order_book_server
    cd order_book_server
else
    echo "✅ Repository already exists, updating..."
    cd order_book_server
    git pull
fi

# Update .env file
echo "⚙️ Configuring environment..."
if [ -f .env ]; then
    cp .env .env.backup
fi

cat > .env << EOF
# Database Configuration (External PostgreSQL)
DATABASE_URL=$DATABASE_URL

# Server Configuration
WEBSOCKET_HOST=0.0.0.0
WEBSOCKET_PORT=8080

# Data Directory Configuration
DATA_DIR=/opt/data/hyperliquid
SNAPSHOTS_DIR=/opt/data/hyperliquid/snapshots
ORDER_DIFFS_DIR=/opt/data/hyperliquid/order_diffs
ORDER_STATUS_DIR=/opt/data/hyperliquid/order_status

# Market Monitor Configuration
MONITOR_ENABLED=true
MONITOR_UPDATE_INTERVAL_SEC=30
MONITOR_THROTTLE_SEC=1

# Logging Configuration
RUST_LOG=info
RUST_BACKTRACE=1

# Performance Configuration
IGNORE_SPOT_TRADING=false
PARALLEL_PROCESSING=true
EOF

# Build the project
echo "🔨 Building the project (this may take 5-10 minutes)..."
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --release

echo ""
echo "🎉 Build complete!"
echo ""
echo "⚠️  CRITICAL: You must run a Hyperliquid node first!"
echo "📖 Read PREREQUISITES.md for setup instructions"
echo ""
echo "📝 To start the server manually:"
echo "   ./target/release/websocket_server --address 0.0.0.0 --port 3000"
echo ""
echo "🔗 After starting, WebSocket will be available at:"
echo "   ws://$(curl -s http://checkip.amazonaws.com):3000/"
