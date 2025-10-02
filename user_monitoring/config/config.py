"""Configuration for Hyperliquid position monitoring system."""
import os
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class MonitorConfig:

    database_url: str
    target_markets: List[str]
    min_position_size_usd: float
    min_position_value_usd: float
    rmp_base_path: Path
    node_binary_path: Path
    snapshots_dir: Path
    active_addresses_file: Path
    data_dir: Path
    nvn_api_url: str
    public_api_url: str
    chain_type: str = "Mainnet"
    api_timeout: int = 10

    snapshot_check_interval: int = 120
    position_refresh_interval: int = 10
    position_refresh_batch_size: int = 500
    max_workers: int = 10
    max_retries: int = 3
    retry_delay: float = 1.0
    snapshot_retention_count: int = 2

    def reload_markets(self) -> bool:
        import os

        markets_str = os.getenv("TARGET_MARKETS", "BTC,ETH,LINK")
        new_markets = [m.strip().upper() for m in markets_str.split(",") if m.strip()]

        if set(new_markets) != set(self.target_markets):
            old_markets = self.target_markets
            self.target_markets = new_markets
            logger.info(f"Markets updated: {old_markets} -> {new_markets}")
            return True
        return False

    @classmethod
    def from_env(cls) -> "MonitorConfig":

        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        markets_str = os.getenv("TARGET_MARKETS", "BTC,ETH,LINK")
        target_markets = [m.strip().upper() for m in markets_str.split(",") if m.strip()]
        home = Path.home()
        rmp_base_path = Path(os.getenv("RMP_BASE_PATH",
                                       f"{home}/hl/data/periodic_abci_states"))
        node_binary_path = Path(os.getenv("NODE_BINARY_PATH", f"{home}/hl-node"))
        snapshots_dir = Path(os.getenv("SNAPSHOTS_DIR",
                                      f"{home}/hl/data/periodic_abci_states"))

        data_dir = Path(os.getenv("DATA_DIR", "./data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        active_addresses_file = data_dir / "active_addresses.txt"
        nvn_api_url = os.getenv("NVN_API_URL", "http://127.0.0.1:3001/info")
        public_api_url = os.getenv("PUBLIC_API_URL", "https://api.hyperliquid.xyz/info")

        config = cls(
            database_url=database_url,
            target_markets=target_markets,
            min_position_size_usd=float(os.getenv("MIN_POSITION_SIZE_USD", "0")),
            min_position_value_usd=float(os.getenv("MIN_POSITION_VALUE_USD", "1.0")),
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

    def reload_from_env(self):

        markets_env = os.getenv("TARGET_MARKETS", "")
        if markets_env:
            old_markets = set(self.target_markets)
            new_markets = [m.strip().upper() for m in markets_env.split(",") if m.strip()]
            self.target_markets = new_markets

            added_markets = set(new_markets) - old_markets
            removed_markets = old_markets - set(new_markets)

            if added_markets:
                logger.info(f"🔄 Hot-reload: Added markets: {', '.join(added_markets)}")
            if removed_markets:
                logger.info(f"🔄 Hot-reload: Removed markets: {', '.join(removed_markets)}")

        new_batch_size = int(os.getenv("POSITION_REFRESH_BATCH_SIZE", "500"))
        if new_batch_size != self.position_refresh_batch_size:
            logger.info(f"🔄 Hot-reload: Batch size {self.position_refresh_batch_size} → {new_batch_size}")
            self.position_refresh_batch_size = new_batch_size

        new_refresh_interval = int(os.getenv("POSITION_REFRESH_INTERVAL", "10"))
        if new_refresh_interval != self.position_refresh_interval:
            logger.info(f"🔄 Hot-reload: Refresh interval {self.position_refresh_interval}s → {new_refresh_interval}s")
            self.position_refresh_interval = new_refresh_interval

        self.validate()
        return added_markets if 'added_markets' in locals() else set()

    def validate(self):
        """Validate configuration settings."""

        if not self.target_markets:
            raise ValueError("At least one target market must be specified")

        if self.min_position_size_usd < 0:
            raise ValueError("MIN_POSITION_SIZE_USD must be non-negative")

        if self.min_position_value_usd < 0:
            raise ValueError("MIN_POSITION_VALUE_USD must be non-negative")

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