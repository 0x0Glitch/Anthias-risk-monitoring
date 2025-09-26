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
                            address = line.lower()
                            self.active_addresses.add(address)
                            for market in self.config.target_markets:
                                self.addresses_by_market[market].add(address)
                        
                    except Exception as e:
                        logger.warning(f"Error processing address line {line}: {e}")
            
            # Log detailed loading results
            total_addresses = len(self.active_addresses)
            logger.info(f"Loaded {total_addresses} active addresses")
            
            for market, addresses in self.addresses_by_market.items():
                if addresses:
                    logger.info(f"  {market}: {len(addresses)} addresses")
                    
            if total_addresses > 0 and all(len(addrs) == 0 for addrs in self.addresses_by_market.values()):
                logger.warning("⚠ Addresses loaded but none assigned to target markets!")
            
        except FileNotFoundError:
            logger.info(f"Creating new active addresses file: {self.addresses_file}")
            self.addresses_file.parent.mkdir(parents=True, exist_ok=True)
            self.addresses_file.touch()
        except Exception as e:
            logger.error(f"Error loading addresses: {e}")
            # Continue with empty set on error
            self.active_addresses = set()
            self.addresses_by_market = {market: set() for market in self.config.target_markets}
    
    async def add_addresses_batch(self, market: str, addresses: set, flush_to_disk: bool = True):
        """Add addresses in batch with immediate persistence"""
        with self.lock:
            # Add to memory
            if market not in self.addresses_by_market:
                self.addresses_by_market[market] = set()
                
            new_addresses = addresses - self.addresses_by_market[market]
            if new_addresses:
                self.addresses_by_market[market].update(new_addresses)
                self.active_addresses.update(new_addresses)
                
                # Immediate disk flush if requested
                if flush_to_disk and len(new_addresses) > 0:
                    self._persist_addresses()
                    
                return len(new_addresses)
        return 0
    
    def _persist_addresses(self):
        """Persist current addresses to disk immediately"""
        try:
            with open(self.addresses_file, 'w') as f:
                for market, addresses in self.addresses_by_market.items():
                    for addr in sorted(addresses):
                        f.write(f"{market}:{addr}\n")
        except Exception as e:
            logger.error(f"Error persisting addresses: {e}")
    
    def _save_to_file(self):
        """Save addresses to persistent file with atomic write."""
        
        tmp_path = Path(str(self.addresses_file) + '.tmp')
        
        try:
            with open(tmp_path, 'w') as f:
                # Write header
                f.write(f"# Active addresses for Hyperliquid monitoring\n")
                f.write(f"# Generated: {datetime.now().isoformat()}\n")
                f.write(f"# Format: market:address\n\n")
                
                # Write addresses sorted by market and address
                for market in sorted(self.addresses_by_market.keys()):
                    addresses = self.addresses_by_market[market]
                    if addresses:
                        f.write(f"# {market} ({len(addresses)} addresses)\n")
                        for address in sorted(addresses):
                            f.write(f"{market}:{address}\n")
                        f.write("\n")
            
            # Atomic replace
            tmp_path.replace(self.addresses_file)
            logger.debug(f"Saved {sum(len(a) for a in self.addresses_by_market.values())} addresses to file")
            
        except Exception as e:
            logger.error(f"Error saving addresses to file: {e}")
            if tmp_path.exists():
                tmp_path.unlink()
    
    async def update_from_snapshot(self, snapshot_addresses: Dict[str, Set[str]]) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        """
        Update addresses from new snapshot with dual-check logic.
        
        Args:
            snapshot_addresses: Dictionary of market -> addresses from snapshot
            
        Returns:
            Tuple of (new_addresses, removal_candidates)
        """
        
        with self.lock:
            new_addresses = {market: set() for market in self.config.target_markets}
            new_removal_candidates = {market: set() for market in self.config.target_markets}
            
            for market in self.config.target_markets:
                current_snapshot = snapshot_addresses.get(market, set())
                current_active = self.addresses_by_market[market]
                previous_snapshot = self.last_snapshot_addresses[market]
                
                # Find new addresses
                new = current_snapshot - current_active
                if new:
                    new_addresses[market] = new
                    self.addresses_by_market[market].update(new)
                    logger.info(f"Added {len(new)} new {market} addresses")
                
                # Find removal candidates (in previous snapshot but not in current)
                if previous_snapshot:
                    # Addresses that were in previous snapshot but not in current
                    disappeared = previous_snapshot - current_snapshot
                    
                    # Only consider for removal if they're currently active
                    candidates = disappeared & current_active
                    
                    if candidates:
                        new_removal_candidates[market] = candidates
                        self.removal_candidates[market].update(candidates)
                        logger.info(f"Marked {len(candidates)} {market} addresses as removal candidates")
                
                # Update last snapshot
                self.last_snapshot_addresses[market] = current_snapshot.copy()
            
            # Save updated addresses to file
            if any(new_addresses.values()):
                self._save_to_file()
            
            return new_addresses, new_removal_candidates
    
    async def confirm_removals(self, closed_positions: Dict[str, Set[str]]):
        """
        Confirm removal of addresses with closed positions (dual-check complete).
        
        Args:
            closed_positions: Dictionary of market -> addresses with closed positions
        """
        
        with self.lock:
            total_removed = 0
            
            for market, closed_addrs in closed_positions.items():
                # Only remove if in removal candidates (dual-check)
                to_remove = closed_addrs & self.removal_candidates[market]
                
                if to_remove:
                    self.addresses_by_market[market] -= to_remove
                    self.removal_candidates[market] -= to_remove
                    total_removed += len(to_remove)
                    
                    logger.info(f"Removed {len(to_remove)} {market} addresses (dual-check confirmed)")
            
            if total_removed > 0:
                self._save_to_file()
    
    def get_addresses(self, market: Optional[str] = None) -> Dict[str, Set[str]]:
        """
        Get active addresses.
        
        Args:
            market: Optional market filter
            
        Returns:
            Dictionary of market -> addresses
        """
        
        with self.lock:
            if market:
                return {market: self.addresses_by_market.get(market, set()).copy()}
            else:
                return {m: addrs.copy() for m, addrs in self.addresses_by_market.items()}
    
    def get_removal_candidates(self, market: Optional[str] = None) -> Dict[str, Set[str]]:
        """Get addresses marked for potential removal."""
        
        with self.lock:
            if market:
                return {market: self.removal_candidates.get(market, set()).copy()}
            else:
                return {m: addrs.copy() for m, addrs in self.removal_candidates.items()}
    
    def get_all_addresses_flat(self) -> List[str]:
        """Get all addresses as a sorted flat list (for sequential API calls)."""
        
        with self.lock:
            all_addresses = set()
            for addresses in self.addresses_by_market.values():
                all_addresses.update(addresses)
            return sorted(list(all_addresses))  # Sort for consistent processing order
    
    def get_address_count(self) -> int:
        """Get total count of unique addresses being tracked."""
        with self.lock:
            return len(self.active_addresses)
    
    async def update_addresses(self, addresses_by_market: Dict[str, Set[str]]) -> int:
        """
        Update addresses from snapshot or other source.
        
        Args:
            addresses_by_market: Dictionary of market -> set of addresses
            
        Returns:
            Number of new addresses added
        """
        
        with self.lock:
            new_count = 0
            
            for market, addresses in addresses_by_market.items():
                if market not in self.addresses_by_market:
                    self.addresses_by_market[market] = set()
                
                # Add new addresses
                before_count = len(self.addresses_by_market[market])
                self.addresses_by_market[market].update(addresses)
                after_count = len(self.addresses_by_market[market])
                new_count += (after_count - before_count)
                
                # Update general active addresses set
                self.active_addresses.update(addresses)
            
            # Save to file if there are changes
            if new_count > 0:
                self._save_to_file()
                logger.info(f"Added {new_count} new addresses from snapshot/update")
            
            return new_count
    
    async def replace_addresses(self, addresses_by_market: Dict[str, Set[str]]) -> Dict[str, int]:
        """
        Replace all addresses with a new set (for snapshot seeding).
        This ensures we only track addresses with active positions.
        
        Args:
            addresses_by_market: Dictionary of market -> set of addresses with active positions
            
        Returns:
            Dictionary with counts: added, removed, total
        """
        
        with self.lock:
            stats = {'added': 0, 'removed': 0, 'total': 0}
            
            # Calculate what's being added and removed
            for market, new_addresses in addresses_by_market.items():
                if market not in self.addresses_by_market:
                    self.addresses_by_market[market] = set()
                
                old_addresses = self.addresses_by_market[market]
                
                # Find additions and removals
                added = new_addresses - old_addresses
                removed = old_addresses - new_addresses
                
                stats['added'] += len(added)
                stats['removed'] += len(removed)
                
                if added:
                    logger.info(f"{market}: Adding {len(added)} new addresses with positions")
                if removed:
                    logger.info(f"{market}: Removing {len(removed)} addresses without positions")
                
                # Replace with new set
                self.addresses_by_market[market] = new_addresses.copy()
            
            # Rebuild general active addresses set from all markets
            self.active_addresses = set()
            for addresses in self.addresses_by_market.values():
                self.active_addresses.update(addresses)
            
            stats['total'] = len(self.active_addresses)
            
            # Always save to file to ensure consistency
            self._save_to_file()
            
            logger.info(f"Address replacement complete: {stats['total']} total addresses "
                       f"(+{stats['added']}/-{stats['removed']})")
            
            # Clear removal candidates since we've done a full replacement
            for market in self.removal_candidates:
                self.removal_candidates[market].clear()
            
            return stats
    
    def get_addresses_by_market(self) -> Dict[str, Set[str]]:
        """
        Get all addresses grouped by market.
        
        Returns:
            Dictionary of market -> set of addresses
        """
        
        with self.lock:
            return {market: addrs.copy() for market, addrs in self.addresses_by_market.items()}
    
    def get_all_addresses(self) -> Set[str]:
        """
        Get all unique addresses across all markets.
        
        Returns:
            Set of all unique addresses
        """
        
        with self.lock:
            all_addresses = set()
            for addresses in self.addresses_by_market.values():
                all_addresses.update(addresses)
            return all_addresses
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about managed addresses."""
        
        with self.lock:
            stats = {
                'total': sum(len(addrs) for addrs in self.addresses_by_market.values()),
                'removal_candidates': sum(len(addrs) for addrs in self.removal_candidates.values())
            }
            
            for market, addresses in self.addresses_by_market.items():
                stats[f'{market.lower()}_active'] = len(addresses)
                stats[f'{market.lower()}_removal_candidates'] = len(self.removal_candidates[market])
            
            return stats
    
    def sync_with_database(self, db_addresses: Dict[str, Set[str]]):
        """
        Ensure consistency between address file and database.
        
        Args:
            db_addresses: Dictionary of market -> addresses from database
        """
        
        with self.lock:
            changes = False
            
            for market in self.config.target_markets:
                file_addrs = self.addresses_by_market[market]
                db_addrs = db_addresses.get(market, set())
                
                # Find discrepancies
                only_in_file = file_addrs - db_addrs
                only_in_db = db_addrs - file_addrs
                
                if only_in_file:
                    logger.warning(f"Found {len(only_in_file)} {market} addresses in file but not in DB")
                    # These should be added to DB in next update cycle
                
                if only_in_db:
                    logger.warning(f"Found {len(only_in_db)} {market} addresses in DB but not in file")
                    # Add to file to maintain consistency
                    self.addresses_by_market[market].update(only_in_db)
                    changes = True
            
            if changes:
                logger.info("Syncing address file with database state")
                self._save_to_file()

