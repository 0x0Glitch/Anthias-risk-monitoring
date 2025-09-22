"""RMP snapshot processor for discovering users with positions."""
import os
import json
import ijson
import asyncio
import subprocess
from pathlib import Path
from typing import Dict, Set, Tuple, Optional, List
from datetime import datetime
import logging

from ..config.config import MonitorConfig


class SnapshotProcessor:
    """Processes RMP snapshots to discover users with positions."""
    
    def __init__(self, config: MonitorConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Paths for RMP processing
        self.rmp_base_path = Path(os.getenv('RMP_BASE_PATH', '~/hl/data/periodic_abci_states')).expanduser()
        self.node_binary_path = Path(os.getenv('NODE_BINARY_PATH', '~/hl-node')).expanduser()
        self.data_dir = config.data_dir
        
        # Ensure data directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Processing configuration
        self.target_markets = set(config.target_markets)
        self.min_position_size = config.min_position_size_usd
        
    async def process_latest_snapshot(self) -> Tuple[bool, Dict[str, Set[str]]]:
        """Process the latest RMP snapshot and return users by market."""
        try:
            # Find latest snapshot file
            latest_snapshot = self._find_latest_snapshot()
            if not latest_snapshot:
                self.logger.warning("No RMP snapshots found")
                return False, {}
                
            # Convert RMP to JSON if needed
            json_file = await self._convert_rmp_to_json(latest_snapshot)
            if not json_file:
                self.logger.error("Failed to convert RMP to JSON")
                return False, {}
                
            # Process JSON to extract users
            users_by_market = await self._extract_users_from_json(json_file)
            
            # Cleanup old JSON files (keep only 2 latest)
            await self._cleanup_old_json_files()
            
            self.logger.info(f"Processed snapshot: {sum(len(users) for users in users_by_market.values())} users found")
            return True, users_by_market
            
        except Exception as e:
            self.logger.error(f"Error processing snapshot: {e}", exc_info=True)
            return False, {}
    
    def _find_latest_snapshot(self) -> Optional[Path]:
        """Find the most recent RMP snapshot file."""
        if not self.rmp_base_path.exists():
            return None
            
        # Look for snapshot files (typically named with timestamps)
        snapshot_files = []
        for file_path in self.rmp_base_path.rglob("*"):
            if file_path.is_file() and not file_path.name.startswith('.'):
                snapshot_files.append(file_path)
                
        if not snapshot_files:
            return None
            
        # Return the most recently modified file
        return max(snapshot_files, key=lambda f: f.stat().st_mtime)
    
    async def _convert_rmp_to_json(self, rmp_file: Path) -> Optional[Path]:
        """Convert RMP file to JSON using hl-node binary."""
        if not self.node_binary_path.exists():
            self.logger.warning(f"hl-node binary not found at {self.node_binary_path}")
            return None
            
        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_file = self.data_dir / f"snapshot_{timestamp}.json"
        
        try:
            # Run hl-node conversion
            process = await asyncio.create_subprocess_exec(
                str(self.node_binary_path),
                'convert-rmp',
                str(rmp_file),
                str(json_file),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                self.logger.error(f"RMP conversion failed: {stderr.decode()}")
                return None
                
            if json_file.exists():
                self.logger.info(f"RMP converted to: {json_file}")
                return json_file
                
        except Exception as e:
            self.logger.error(f"Error running hl-node: {e}")
            
        return None
    
    async def _extract_users_from_json(self, json_file: Path) -> Dict[str, Set[str]]:
        """Extract users with positions from JSON snapshot."""
        users_by_market = {market: set() for market in self.target_markets}
        
        try:
            with open(json_file, 'rb') as f:
                # Stream parse the JSON file for memory efficiency
                parser = ijson.parse(f)
                current_address = None
                current_market = None
                current_position_size = 0
                
                for prefix, event, value in parser:
                    try:
                        # Parse address field
                        if prefix.endswith('.address') and event == 'string':
                            current_address = value
                            
                        # Parse market/coin field
                        elif prefix.endswith('.coin') and event == 'string':
                            if value in self.target_markets:
                                current_market = value
                            else:
                                current_market = None
                                
                        # Parse position size
                        elif prefix.endswith('.szi') and event in ('string', 'number'):
                            if current_market and current_address:
                                try:
                                    position_size = abs(float(value)) if value else 0
                                    
                                    # Check if position meets minimum size threshold
                                    if position_size * 1000 >= self.min_position_size:  # Rough estimate
                                        users_by_market[current_market].add(current_address)
                                        
                                except (ValueError, TypeError):
                                    pass
                                    
                    except Exception as e:
                        self.logger.debug(f"Parse error: {e}")
                        continue
                        
        except Exception as e:
            self.logger.error(f"Error parsing JSON file: {e}")
            
        return users_by_market
    
    async def _cleanup_old_json_files(self):
        """Keep only the 2 most recent JSON files."""
        try:
            json_files = list(self.data_dir.glob("snapshot_*.json"))
            
            if len(json_files) > 2:
                # Sort by modification time and remove oldest
                json_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
                
                for old_file in json_files[2:]:
                    old_file.unlink()
                    self.logger.debug(f"Removed old snapshot: {old_file}")
                    
        except Exception as e:
            self.logger.error(f"Error cleaning up JSON files: {e}")
