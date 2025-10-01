"""
Production configuration for Hyperliquid position monitoring system.
All settings are configurable via environment variables with sensible defaults.
"""
import os
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class MonitorConfig:
    """Main configuration for the monitoring system."""
    
    # Database
    database_url: str
    
    # Target markets and thresholds
    target_markets: List[str]
    min_position_size_usd: float
    
    # RMP snapshot paths
    rmp_base_path: Path
    node_binary_path: Path
    snapshots_dir: Path  # Directory containing RMP snapshot files
    
    # File paths (non-default fields must come before default fields)
    active_addresses_file: Path
    data_dir: Path
    
    # API endpoints
    nvn_api_url: str
    public_api_url: str
    
    # Fields with default values must come after non-default fields
    chain_type: str = "Mainnet"
    api_timeout: int = 10
    
    # Processing intervals (seconds)
    snapshot_check_interval: int = 120  # Check for new snapshots every 2 minutes
    position_refresh_interval: int = 10  # Refresh positions every 10 seconds
    position_refresh_batch_size: int = 500  # Process 50 addresses per batch
    
    # Worker settings
    max_workers: int = 10
    
    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Cleanup settings
    snapshot_retention_count: int = 2  # Keep only 2 latest JSON snapshots
    
    @classmethod
    def from_env(cls) -> "MonitorConfig":
        """Create configuration from environment variables."""
        
        # Required environment variables
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        # Parse target markets
        markets_str = os.getenv("TARGET_MARKETS", "BTC,ETH,LINK")
        target_markets = [m.strip().upper() for m in markets_str.split(",") if m.strip()]
        
        # Paths
        home = Path.home()
        rmp_base_path = Path(os.getenv("RMP_BASE_PATH", 
                                       f"{home}/hl/data/periodic_abci_states"))
        node_binary_path = Path(os.getenv("NODE_BINARY_PATH", f"{home}/hl-node"))
        snapshots_dir = Path(os.getenv("SNAPSHOTS_DIR", 
                                      f"{home}/hl/data/periodic_abci_states"))
        
        # Data directory
        data_dir = Path(os.getenv("DATA_DIR", "./data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # Active addresses file
        active_addresses_file = data_dir / "active_addresses.txt"
        
        # API endpoints
        nvn_api_url = os.getenv("NVN_API_URL", "http://127.0.0.1:3001/info")
        public_api_url = os.getenv("PUBLIC_API_URL", "https://api.hyperliquid.xyz/info")
        
        config = cls(
            database_url=database_url,
            target_markets=target_markets,
            min_position_size_usd=float(os.getenv("MIN_POSITION_SIZE_USD", "0")),
            rmp_base_path=rmp_base_path,
            node_binary_path=node_binary_path,
            snapshots_dir=snapshots_dir,
            chain_type=os.getenv("CHAIN_TYPE", "Mainnet"),
            active_addresses_file=active_addresses_file,
            data_dir=data_dir,
            nvn_api_url=nvn_api_url,
            public_api_url=public_api_url,
            api_timeout=int(os.getenv("API_TIMEOUT", "10")),
            snapshot_check_interval=int(os.getenv("SNAPSHOT_CHECK_INTERVAL", "120")),
            position_refresh_interval=int(os.getenv("POSITION_REFRESH_INTERVAL", "10")),
            position_refresh_batch_size=int(os.getenv("POSITION_REFRESH_BATCH_SIZE", "500")),
            max_workers=int(os.getenv("MAX_WORKERS", "10")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_delay=float(os.getenv("RETRY_DELAY", "1.0")),
            snapshot_retention_count=int(os.getenv("SNAPSHOT_RETENTION_COUNT", "2"))
        )
        
        config.validate()
        return config
    
    def validate(self):
        """Validate configuration settings."""
        
        if not self.target_markets:
            raise ValueError("At least one target market must be specified")
        
        if self.min_position_size_usd < 0:
            raise ValueError("MIN_POSITION_SIZE_USD must be non-negative")
        
        if not self.node_binary_path.exists():
            logger.warning(f"Node binary not found at {self.node_binary_path}")
            logger.warning("RMP conversion will fail. Please ensure hl-node is installed")
        
        if not self.rmp_base_path.exists():
            logger.warning(f"RMP base path does not exist: {self.rmp_base_path}")
            logger.warning("Creating directory...")
            self.rmp_base_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Configuration loaded successfully")
        logger.info(f"Target markets: {', '.join(self.target_markets)}")
        logger.info(f"Min position size: ${self.min_position_size_usd:,.2f}")
        logger.info(f"RMP path: {self.rmp_base_path}")
        logger.info(f"Data directory: {self.data_dir}")
