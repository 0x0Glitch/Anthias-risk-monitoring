"""
Active address manager with file persistence and synchronization.
Maintains the source of truth for addresses being monitored.
"""
import asyncio
import logging
from pathlib import Path
from typing import Set, Dict, Optional, List, Tuple
from datetime import datetime
from threading import Lock

logger = logging.getLogger(__name__)


class AddressManager:
    """
    Manages active addresses with file persistence.
    Ensures consistency between file and memory.
    """
    
    def __init__(self, config):
        self.config = config
        self.addresses_file = config.active_addresses_file  # Fix: use addresses_file consistently
        self.lock = Lock()
        
        # In-memory cache: market -> set of addresses
        self.addresses_by_market: Dict[str, Set[str]] = {
            market: set() for market in config.target_markets
        }
        
        # General active addresses set
        self.active_addresses: Set[str] = set()
        
        # Track removal candidates (addresses not in latest snapshot)
        self.removal_candidates: Dict[str, Set[str]] = {
            market: set() for market in config.target_markets
        }
        
        # Track snapshot history for dual-check
        self.last_snapshot_addresses: Dict[str, Set[str]] = {
            market: set() for market in config.target_markets
        }
        
        # Load existing addresses from file
        self._load_addresses()
    
    def _is_valid_address(self, address: str) -> bool:
        """Check if address is a valid Ethereum address."""
        import re
        if not isinstance(address, str):
            return False
        
        # Remove any market prefix (e.g., "LINK:0x...")
        if ':' in address:
            address = address.split(':', 1)[1]
        
        # Check if it's a valid Ethereum address format
        return bool(re.match(r'^0x[a-fA-F0-9]{40}$', address.strip()))
    
    def _load_addresses(self):
        """Load addresses from file."""
        
        if not self.addresses_file.exists():
            logger.info(f"Creating new active addresses file: {self.addresses_file}")
            self.addresses_file.parent.mkdir(parents=True, exist_ok=True)
            self.addresses_file.touch()
            return
        
        try:
            with open(self.addresses_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    
                    try:
                        # Parse market:address format
                        if ':' in line:
                            # Format: "LINK:0x..."
                            market, address = line.split(':', 1)
                            market = market.upper().strip()
                            address = address.lower().strip()
                            
                            # Validate address format
                            if not self._is_valid_address(address):
                                logger.warning(f"Invalid address format: {address}")
                                continue
                            
                            # Add to market-specific set
                            if market in self.config.target_markets:
                                self.addresses_by_market[market].add(address)
                                self.active_addresses.add(address)
                                
                        else:
                            # Old format: just address
                            if not self._is_valid_address(line):
                                logger.warning(f"Invalid address format: {line}")
                                continue
                            
                            # Add to general set and all target markets
                            address = line.lower().strip()
                            self.active_addresses.add(address)
                            for market in self.config.target_markets:
                                self.addresses_by_market[market].add(address)
                    
                    except ValueError as e:
                        logger.warning(f"Error parsing line '{line}': {e}")
                        continue
            
            total_addresses = len(self.active_addresses)
            by_market = {market: len(addrs) for market, addrs in self.addresses_by_market.items()}
            logger.info(f"Loaded {total_addresses} addresses: {by_market}")
            
        except Exception as e:
            logger.error(f"Failed to load addresses from {self.addresses_file}: {e}")
    
    def _save_addresses(self):
        """Save addresses to file in a consistent format."""
        
        try:
            # Write to temp file first for atomicity
            temp_file = self.addresses_file.with_suffix('.tmp')
            
            with open(temp_file, 'w') as f:
                f.write(f"# Active addresses for Hyperliquid position monitoring\n")
                f.write(f"# Generated: {datetime.now().isoformat()}\n")
                f.write(f"# Total unique addresses: {len(self.active_addresses)}\n")
                f.write(f"# Format: MARKET:address\n\n")
                
                for market in sorted(self.config.target_markets):
                    addresses = self.addresses_by_market[market]
                    if addresses:
                        f.write(f"# {market} ({len(addresses)} addresses)\n")
                        for address in sorted(addresses):
                            f.write(f"{market}:{address}\n")
                        f.write("\n")
            
            # Atomic replacement
            temp_file.replace(self.addresses_file)
            logger.debug(f"Saved {len(self.active_addresses)} addresses to file")
            
        except Exception as e:
            logger.error(f"Failed to save addresses to {self.addresses_file}: {e}")
    
    def get_addresses_by_market(self) -> Dict[str, Set[str]]:
        """Get current addresses organized by market."""
        with self.lock:
            return {market: addresses.copy() for market, addresses in self.addresses_by_market.items()}
    
    def get_all_addresses(self) -> Set[str]:
        """Get all unique addresses across all markets."""
        with self.lock:
            return self.active_addresses.copy()
    
    def get_address_count(self) -> Dict[str, int]:
        """Get count of addresses per market."""
        with self.lock:
            return {
                market: len(addresses) 
                for market, addresses in self.addresses_by_market.items()
            }
    
    async def replace_addresses(self, users_by_market: Dict[str, Set[str]]) -> Dict[str, int]:
        """
        REPLACE ALL ADDRESSES with those from snapshot.
        This is the FULL REPLACE approach - we trust the snapshot completely.
        
        Args:
            users_by_market: New addresses from snapshot processing
            
        Returns:
            Dict with stats: added, removed, total
        """
        
        with self.lock:
            # Track what we had before
            old_addresses = self.active_addresses.copy()
            old_by_market = {market: addresses.copy() for market, addresses in self.addresses_by_market.items()}
            
            # CLEAR EVERYTHING and replace with snapshot data
            self.active_addresses.clear()
            for market in self.config.target_markets:
                self.addresses_by_market[market].clear()
            
            # Add new addresses from snapshot
            for market, addresses in users_by_market.items():
                if market in self.config.target_markets:
                    # Add to market-specific set
                    self.addresses_by_market[market].update(addresses)
                    # Add to general active set
                    self.active_addresses.update(addresses)
            
            # Calculate stats
            new_addresses = self.active_addresses.copy()
            
            added = new_addresses - old_addresses
            removed = old_addresses - new_addresses
            
            stats = {
                'added': len(added),
                'removed': len(removed),
                'total': len(new_addresses)
            }
            
            # Update removal candidates
            self.removal_candidates = {market: set() for market in self.config.target_markets}
            
            # Save to file
            self._save_addresses()
            
            logger.info(f"🔄 REPLACED ALL ADDRESSES: +{stats['added']} -{stats['removed']} = {stats['total']} total")
            
            # Log by market for debugging
            for market in self.config.target_markets:
                old_count = len(old_by_market.get(market, set()))
                new_count = len(self.addresses_by_market[market])
                if old_count != new_count:
                    logger.info(f"  {market}: {old_count} -> {new_count} ({new_count - old_count:+d})")
            
            return stats
    
    async def add_addresses(self, market: str, addresses: Set[str]) -> int:
        """Add new addresses to a market."""
        
        if market not in self.config.target_markets:
            logger.warning(f"Cannot add addresses to non-target market: {market}")
            return 0
        
        if not addresses:
            return 0
        
        # Validate addresses
        valid_addresses = set()
        for address in addresses:
            if self._is_valid_address(address):
                valid_addresses.add(address.lower().strip())
            else:
                logger.warning(f"Invalid address format: {address}")
        
        if not valid_addresses:
            return 0
        
        with self.lock:
            # Track what's new
            new_addresses = valid_addresses - self.addresses_by_market[market]
            
            if new_addresses:
                # Add to market set
                self.addresses_by_market[market].update(new_addresses)
                # Add to general set
                self.active_addresses.update(new_addresses)
                
                # Save to file
                self._save_addresses()
                
                logger.info(f"Added {len(new_addresses)} new addresses to {market}")
                return len(new_addresses)
            else:
                logger.debug(f"No new addresses to add for {market}")
                return 0
    
    async def remove_addresses(self, market: str, addresses: Set[str]) -> int:
        """Remove addresses from a market."""
        
        if market not in self.config.target_markets:
            logger.warning(f"Cannot remove addresses from non-target market: {market}")
            return 0
        
        if not addresses:
            return 0
        
        # Normalize addresses
        normalized_addresses = {addr.lower().strip() for addr in addresses}
        
        with self.lock:
            # Track what's actually being removed
            to_remove = normalized_addresses & self.addresses_by_market[market]
            
            if to_remove:
                # Remove from market set
                self.addresses_by_market[market] -= to_remove
                
                # Remove from general set only if not in any other market
                for address in to_remove:
                    if not any(address in self.addresses_by_market[m] for m in self.config.target_markets):
                        self.active_addresses.discard(address)
                
                # Save to file
                self._save_addresses()
                
                logger.info(f"Removed {len(to_remove)} addresses from {market}")
                return len(to_remove)
            else:
                logger.debug(f"No addresses to remove for {market}")
                return 0
    
    def mark_for_removal(self, market: str, addresses: Set[str]):
        """Mark addresses as candidates for removal."""
        
        if market not in self.config.target_markets:
            return
        
        normalized = {addr.lower().strip() for addr in addresses}
        
        with self.lock:
            self.removal_candidates[market].update(normalized)
            logger.debug(f"Marked {len(normalized)} addresses for removal in {market}")
    
    def unmark_for_removal(self, market: str, addresses: Set[str]):
        """Remove addresses from removal candidates."""
        
        if market not in self.config.target_markets:
            return
        
        normalized = {addr.lower().strip() for addr in addresses}
        
        with self.lock:
            self.removal_candidates[market] -= normalized
            logger.debug(f"Unmarked {len(normalized)} addresses from removal in {market}")
    
    def get_removal_candidates(self) -> Dict[str, Set[str]]:
        """Get current removal candidates by market."""
        with self.lock:
            return {
                market: candidates.copy() 
                for market, candidates in self.removal_candidates.items()
            }
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about managed addresses."""
        with self.lock:
            stats = {
                'total_unique': len(self.active_addresses),
                'total_removal_candidates': sum(len(candidates) for candidates in self.removal_candidates.values())
            }
            
            for market in self.config.target_markets:
                stats[f'{market}_addresses'] = len(self.addresses_by_market[market])
                stats[f'{market}_removal_candidates'] = len(self.removal_candidates[market])
            
            return stats
