"""Constants and enums for Hyperliquid Position Monitoring System."""
from enum import Enum
from typing import Set

# =============================================================================
# SYSTEM ADDRESSES
# =============================================================================

SYSTEM_ADDRESSES: Set[str] = {
    '0x0000000000000000000000000000000000000000',
    '0x0000000000000000000000000000000000000001',
    '0x000000000000000000000000000000000000dead',
    '0xffffffffffffffffffffffffffffffffffffffff',
}


# =============================================================================
# VALIDATION CONSTANTS
# =============================================================================

ADDRESS_LENGTH = 42
ADDRESS_PREFIX = '0x'
HEX_CHARS = '0123456789abcdefABCDEF'
MIN_RMP_FILE_SIZE = 1000
MIN_JSON_FILE_SIZE = 1000


# =============================================================================
# DATABASE CONSTANTS
# =============================================================================

class DatabaseConfig:
    MIN_POOL_SIZE = 5
    MAX_POOL_SIZE = 20
    COMMAND_TIMEOUT = 60
    MAX_BATCH_SIZE = 100
    INSERT_CHUNK_SIZE = 500
    CLOSED_POSITION_MAX_AGE_HOURS = 24
    STALE_POSITION_MAX_AGE_HOURS = 168


# =============================================================================
# API CONSTANTS
# =============================================================================

class APIConfig:
    DEFAULT_HTTP_CONCURRENCY = 5
    API_CALL_DELAY = 0.1
    RETRY_BACKOFF_SEC = 0.5
    POSITION_BATCH_SIZE = 500
    BATCH_TIMEOUT = 30.0
    BATCH_DELAY = 0.5
    BATCH_ERROR_DELAY = 2.0


# =============================================================================
# PROCESSING INTERVALS
# =============================================================================

class ProcessingIntervals:
    SNAPSHOT_CHECK = 120
    POSITION_REFRESH = 10
    HEALTH_MONITOR = 30
    STATS_REPORT = 300
    CLEANUP = 3600
    SNAPSHOT_COOLDOWN = 30


# =============================================================================
# FILE MANAGEMENT
# =============================================================================

class FileConfig:
    MAX_SNAPSHOT_CACHE_SIZE = 100
    SNAPSHOT_RETENTION_COUNT = 2
    FILE_READ_CHUNK_SIZE = 500 * 1024 * 1024
    HASH_BLOCK_SIZE = 4096


# =============================================================================
# LOGGING CONSTANTS
# =============================================================================

class LogConfig:
    MAX_LOG_SIZE = 10 * 1024 * 1024
    MAX_ERROR_LOG_SIZE = 5 * 1024 * 1024
    LOG_BACKUP_COUNT = 5
    ERROR_LOG_BACKUP_COUNT = 3


# =============================================================================
# MONITORING THRESHOLDS
# =============================================================================

class MonitoringThresholds:
    MAX_CONSECUTIVE_ERRORS = 5
    MAX_TASK_ERRORS = 10
    MAX_DEGRADED_COMPONENTS = 2
    CONVERSION_TIMEOUT = 300
    SUBPROCESS_BUFFER_LIMIT = 1024 * 1024 * 10
    SHUTDOWN_TIMEOUT = 10.0


# =============================================================================
# ENUMS
# =============================================================================

class SystemState(Enum):
    INITIALIZING = "initializing"
    READY = "ready"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    FATAL = "fatal"


class ProcessingStatus(Enum):
    """Snapshot processing status."""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class APISource(Enum):
    """API source enumeration."""
    NVN = "nvn"
    PUBLIC = "public"


class LeverageType(Enum):
    """Position leverage type."""
    CROSS = "cross"
    ISOLATED = "isolated"


# =============================================================================
# CHAIN CONFIGURATION
# =============================================================================

class ChainConfig:
    """Blockchain configuration."""
    DEFAULT_CHAIN_TYPE = "Mainnet"
    SUPPORTED_CHAINS = {"Mainnet", "Testnet"}