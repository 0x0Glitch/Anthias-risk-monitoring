"""Basic configuration loading from environment variables."""
import os
from pathlib import Path


class MonitorConfig:
    """Configuration class for the monitor."""
    
    def __init__(self):
        self.data_dir = Path(os.getenv('DATA_DIR', './data'))
        self.target_markets = os.getenv('TARGET_MARKETS', 'BTC,ETH').split(',')
        self.min_position_size_usd = int(os.getenv('MIN_POSITION_SIZE_USD', '10000'))
    
    @classmethod
    def from_env(cls):
        """Create config from environment variables."""
        return cls()
