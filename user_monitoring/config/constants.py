"""
Constants and enums for Hyperliquid Position Monitoring System.
Centralizes all magic numbers, system addresses, and configuration values.
"""
from enum import Enum
from typing import Set

# =============================================================================
# SYSTEM ADDRESSES
# =============================================================================

# Ethereum addresses to filter out (system accounts, burn addresses)
SYSTEM_ADDRESSES: Set[str] = {
    '0x0000000000000000000000000000000000000000',  # Zero address
    '0x0000000000000000000000000000000000000001',  # Genesis address
    '0x000000000000000000000000000000000000dead',  # Burn address
    '0xffffffffffffffffffffffffffffffffffffffff',  # Max address
}


# =============================================================================
# VALIDATION CONSTANTS
# =============================================================================

# Address validation
ADDRESS_LENGTH = 42  # Including '0x' prefix
ADDRESS_PREFIX = '0x'
HEX_CHARS = '0123456789abcdefABCDEF'

# Minimum file sizes for validation
MIN_RMP_FILE_SIZE = 1000  # bytes
MIN_JSON_FILE_SIZE = 1000  # bytes


# =============================================================================
# DATABASE CONSTANTS
# =============================================================================

class DatabaseConfig:
    """Database-related constants."""
    # Connection pool settings
    MIN_POOL_SIZE = 5
    MAX_POOL_SIZE = 20
    COMMAND_TIMEOUT = 60  # seconds
    
    # Batch processing
    MAX_BATCH_SIZE = 100
    INSERT_CHUNK_SIZE = 500
    
    # Cleanup thresholds
    CLOSED_POSITION_MAX_AGE_HOURS = 24
    STALE_POSITION_MAX_AGE_HOURS = 168  # 1 week


# =============================================================================
# API CONSTANTS
# =============================================================================

class APIConfig:
    """API-related constants."""
    # Rate limiting
    DEFAULT_HTTP_CONCURRENCY = 5
    API_CALL_DELAY = 0.1  # seconds between calls
    RETRY_BACKOFF_SEC = 0.5
    
    # Batch sizes
    POSITION_BATCH_SIZE = 500
    BATCH_TIMEOUT = 30.0  # seconds
    BATCH_DELAY = 0.5  # seconds between batches
    
    # Error handling
    BATCH_ERROR_DELAY = 2.0  # seconds after batch errors


# =============================================================================
# PROCESSING INTERVALS
# =============================================================================

class ProcessingIntervals:
    """Time intervals for various tasks (in seconds)."""
    SNAPSHOT_CHECK = 120  # Check for new snapshots
    POSITION_REFRESH = 10  # Refresh positions
    HEALTH_MONITOR = 30  # Health checks
    STATS_REPORT = 300  # Statistics reporting
    CLEANUP = 3600  # Cleanup tasks
    SNAPSHOT_COOLDOWN = 30  # Cooldown between snapshot attempts


# =============================================================================
# FILE MANAGEMENT
# =============================================================================

class FileConfig:
    """File management constants."""
    # Snapshot retention
    MAX_SNAPSHOT_CACHE_SIZE = 100
    SNAPSHOT_RETENTION_COUNT = 2  # Keep only 2 latest JSON snapshots
    
    # File operations
    FILE_READ_CHUNK_SIZE = 500 * 1024 * 1024  # 500MB chunks
    HASH_BLOCK_SIZE = 4096  # bytes for hashing


# =============================================================================
# LOGGING CONSTANTS
# =============================================================================

class LogConfig:
    """Logging configuration constants."""
    # File sizes
    MAX_LOG_SIZE = 10 * 1024 * 1024  # 10MB
    MAX_ERROR_LOG_SIZE = 5 * 1024 * 1024  # 5MB
    
    # Backup counts
    LOG_BACKUP_COUNT = 5
    ERROR_LOG_BACKUP_COUNT = 3


# =============================================================================
# MONITORING THRESHOLDS
# =============================================================================

class MonitoringThresholds:
    """System health and monitoring thresholds."""
    # Component health
    MAX_CONSECUTIVE_ERRORS = 5
    MAX_TASK_ERRORS = 10
    MAX_DEGRADED_COMPONENTS = 2
    
    # Timeouts
    CONVERSION_TIMEOUT = 300  # seconds for RMP conversion
    SUBPROCESS_BUFFER_LIMIT = 1024 * 1024 * 10  # 10MB
    SHUTDOWN_TIMEOUT = 10.0  # seconds for graceful shutdown


# =============================================================================
# ENUMS
# =============================================================================

class SystemState(Enum):
    """System state enumeration."""
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
