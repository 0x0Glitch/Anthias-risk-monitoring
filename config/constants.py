"""System constants and configuration defaults."""
from enum import Enum
from typing import Dict, List


class SystemState(Enum):
    """System state enumeration."""
    INITIALIZING = "initializing"
    READY = "ready"
    RUNNING = "running"
    DEGRADED = "degraded"
    ERROR = "error"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FATAL = "fatal"


class ProcessingIntervals:
    """Default processing intervals in seconds."""
    SNAPSHOT_CHECK = 60
    POSITION_REFRESH = 10
    HEALTH_CHECK = 30
    STATS_REPORT = 300
    CLEANUP = 3600
    SNAPSHOT_COOLDOWN = 30


class MonitoringThresholds:
    """Monitoring and health check thresholds."""
    MAX_CONSECUTIVE_ERRORS = 5
    MAX_TASK_ERRORS = 3
    MAX_DEGRADED_COMPONENTS = 1
    API_TIMEOUT_SECONDS = 30
    DB_TIMEOUT_SECONDS = 10
    MAX_MEMORY_MB = 1024
    MAX_CPU_PERCENT = 80.0


class ApiEndpoints:
    """Default API endpoints."""
    LOCAL_NODE = "http://127.0.0.1:3001/info"
    PUBLIC_API = "https://api.hyperliquid.xyz/info"


# Margin tier configuration for liquidation calculations
MARGIN_TIERS: Dict[str, List[Dict]] = {
    "BTC": [
        {"min_value": 0, "max_value": 50000, "max_leverage": 50},
        {"min_value": 50000, "max_value": 100000, "max_leverage": 25},
        {"min_value": 100000, "max_value": float('inf'), "max_leverage": 10}
    ],
    "ETH": [
        {"min_value": 0, "max_value": 50000, "max_leverage": 50},
        {"min_value": 50000, "max_value": 100000, "max_leverage": 25},
        {"min_value": 100000, "max_value": float('inf'), "max_leverage": 10}
    ],
    "DEFAULT": [
        {"min_value": 0, "max_value": 25000, "max_leverage": 20},
        {"min_value": 25000, "max_value": 50000, "max_leverage": 10},
        {"min_value": 50000, "max_value": float('inf'), "max_leverage": 5}
    ]
}

# Default markets to monitor
DEFAULT_MARKETS = ["BTC", "ETH", "LINK"]

# File patterns and naming
SNAPSHOT_FILE_PATTERN = "snapshot_*.json"
LOG_FILE_PATTERN = "monitor_*.log"
STATS_FILE_NAME = "monitor_stats.json"
ADDRESSES_FILE_NAME = "active_addresses.txt"

# Database configuration
DEFAULT_DB_POOL_SIZE = {"min": 2, "max": 10}
DEFAULT_BATCH_SIZE = 50

# Network configuration
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 1.0

# System resource limits
MEMORY_WARNING_THRESHOLD_MB = 512
MEMORY_CRITICAL_THRESHOLD_MB = 768
CPU_WARNING_THRESHOLD_PERCENT = 60.0
CPU_CRITICAL_THRESHOLD_PERCENT = 80.0
