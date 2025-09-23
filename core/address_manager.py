"""Address manager for maintaining active addresses with file persistence."""
import asyncio
import logging
from pathlib import Path
from typing import Dict, Set, List, Optional, Any
from datetime import datetime
from collections import defaultdict

from ..config.config import MonitorConfig


class AddressManager:
    """Manages active addresses with persistent file storage."""
    
    def __init__(self, config: MonitorConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # File for storing active addresses
        self.addresses_file = config.data_dir / "active_addresses.txt"
        self.temp_addresses_file = config.data_dir / "active_addresses.tmp"
        
        # In-memory storage
        self.addresses_by_market: Dict[str, Set[str]] = defaultdict(set)
        self.removal_candidates: Dict[str, datetime] = {}
        
        # Ensure data directory exists
        config.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing addresses
        self._load_addresses_from_file()
    
    def get_addresses_by_market(self) -> Dict[str, Set[str]]:
        """Get current addresses organized by market."""
        return dict(self.addresses_by_market)
    
    def get_all_addresses(self) -> Set[str]:
        """Get all unique addresses across markets."""
        all_addresses = set()
        for addresses in self.addresses_by_market.values():
            all_addresses.update(addresses)
        return all_addresses
    
    async def replace_addresses(self, new_addresses_by_market: Dict[str, Set[str]]) -> Dict[str, Any]:
        """Replace all addresses with new set from snapshot."""
        stats = {
            'added': 0,
            'removed': 0,
            'total': 0
        }
        
        # Track current addresses for comparison
        old_addresses = self.get_all_addresses()
        
        # Replace addresses
        self.addresses_by_market.clear()
        for market, addresses in new_addresses_by_market.items():
            if market in self.config.target_markets:
                self.addresses_by_market[market] = set(addresses)
        
        # Calculate stats
        new_addresses = self.get_all_addresses()
        stats['added'] = len(new_addresses - old_addresses)
        stats['removed'] = len(old_addresses - new_addresses)
        stats['total'] = len(new_addresses)
        
        # Persist to file
        await self._save_addresses_to_file()
        
        self.logger.info(f"Address update: +{stats['added']} -{stats['removed']} = {stats['total']} total")
        return stats
    
    async def add_addresses(self, market: str, addresses: Set[str]) -> int:
        """Add new addresses for a market."""
        if market not in self.config.target_markets:
            return 0
            
        initial_count = len(self.addresses_by_market[market])
        self.addresses_by_market[market].update(addresses)
        final_count = len(self.addresses_by_market[market])
        
        added_count = final_count - initial_count
        
        if added_count > 0:
            await self._save_addresses_to_file()
            self.logger.info(f"Added {added_count} addresses for {market}")
            
        return added_count
    
    async def remove_addresses(self, market: str, addresses: Set[str]) -> int:
        """Remove addresses for a market."""
        if market not in self.addresses_by_market:
            return 0
            
        initial_count = len(self.addresses_by_market[market])
        self.addresses_by_market[market] -= addresses
        final_count = len(self.addresses_by_market[market])
        
        removed_count = initial_count - final_count
        
        if removed_count > 0:
            await self._save_addresses_to_file()
            self.logger.info(f"Removed {removed_count} addresses for {market}")
            
        return removed_count
    
    def mark_for_removal(self, address: str, market: str):
        """Mark an address as candidate for removal."""
        key = f"{address}:{market}"
        if key not in self.removal_candidates:
            self.removal_candidates[key] = datetime.now()
            self.logger.debug(f"Marked for removal: {key}")
    
    def unmark_for_removal(self, address: str, market: str):
        """Remove address from removal candidates."""
        key = f"{address}:{market}"
        if key in self.removal_candidates:
            del self.removal_candidates[key]
            self.logger.debug(f"Unmarked for removal: {key}")
    
    def get_removal_candidates(self, max_age_hours: int = 2) -> List[str]:
        """Get addresses marked for removal longer than max_age_hours."""
        cutoff_time = datetime.now().timestamp() - (max_age_hours * 3600)
        candidates = []
        
        for key, marked_time in self.removal_candidates.items():
            if marked_time.timestamp() < cutoff_time:
                candidates.append(key)
                
        return candidates
    
    def _load_addresses_from_file(self):
        """Load addresses from persistent file."""
        if not self.addresses_file.exists():
            self.logger.info("No existing addresses file found")
            return
            
        try:
            with open(self.addresses_file, 'r') as f:
                current_market = None
                
                for line in f:
                    line = line.strip()
                    
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                        
                    # Check for market headers
                    if line.startswith('# ') and '(' in line and ')' in line:
                        # Extract market name from header like "# BTC (150 addresses)"
                        market = line.split('(')[0].replace('# ', '').strip()
                        if market in self.config.target_markets:
                            current_market = market
                        else:
                            current_market = None
                        continue
                        
                    # Parse address lines
                    if ':' in line and current_market:
                        try:
                            market, address = line.split(':', 1)
                            if market == current_market and market in self.config.target_markets:
                                self.addresses_by_market[market].add(address)
                        except ValueError:
                            self.logger.warning(f"Invalid address line: {line}")
                            
            total_addresses = sum(len(addrs) for addrs in self.addresses_by_market.values())
            self.logger.info(f"Loaded {total_addresses} addresses from file")
            
        except Exception as e:
            self.logger.error(f"Error loading addresses from file: {e}")
    
    async def _save_addresses_to_file(self):
        """Save addresses to persistent file with atomic write."""
        try:
            # Write to temporary file first
            with open(self.temp_addresses_file, 'w') as f:
                f.write("# Active addresses for Hyperliquid monitoring\n")
                f.write(f"# Generated: {datetime.now().isoformat()}\n")
                f.write("# Format: market:address\n\n")
                
                for market in sorted(self.config.target_markets):
                    addresses = self.addresses_by_market.get(market, set())
                    if addresses:
                        f.write(f"# {market} ({len(addresses)} addresses)\n")
                        for address in sorted(addresses):
                            f.write(f"{market}:{address}\n")
                        f.write("\n")
            
            # Atomic move to final location
            self.temp_addresses_file.replace(self.addresses_file)
            
            total_addresses = sum(len(addrs) for addrs in self.addresses_by_market.values())
            self.logger.debug(f"Saved {total_addresses} addresses to file")
            
        except Exception as e:
            self.logger.error(f"Error saving addresses to file: {e}")
            # Clean up temp file on error
            if self.temp_addresses_file.exists():
                self.temp_addresses_file.unlink()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get address manager statistics."""
        total_addresses = sum(len(addrs) for addrs in self.addresses_by_market.values())
        
        return {
            'total_addresses': total_addresses,
            'addresses_by_market': {
                market: len(addresses) 
                for market, addresses in self.addresses_by_market.items()
            },
            'removal_candidates': len(self.removal_candidates),
            'file_exists': self.addresses_file.exists(),
            'file_size_bytes': self.addresses_file.stat().st_size if self.addresses_file.exists() else 0
        }
