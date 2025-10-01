"""
Dynamic market-specific address manager with per-market file persistence.
Maintains separate {market}_addresses.txt files for each monitored market.
Mirrors the single-market implementation logic but extends to multiple markets.
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
    Manages active addresses with market-specific file persistence.
    Maintains separate address files for each market (btc_addresses.txt, eth_addresses.txt, etc.)
    Uses the EXACT same dual-check logic as single-market version.
    """

    def __init__(self, config):
        self.config = config
        self.data_dir = config.data_dir
        self.file_lock = asyncio.Lock()
        self.lock = Lock()  # Threading lock for thread-safe operations

        # Initialize market-specific structures
        self.addresses_by_market: Dict[str, Set[str]] = {}
        self.removal_candidates: Dict[str, Set[str]] = {}
        self.last_snapshot_addresses: Dict[str, Set[str]] = {}

        # General active addresses set (union of all markets)
        self.active_addresses: Set[str] = set()

        # Initialize for all target markets
        for market in config.target_markets:
            self.addresses_by_market[market] = set()
            self.removal_candidates[market] = set()
            self.last_snapshot_addresses[market] = set()

        # Load existing addresses from market-specific files
        self._load_all_market_addresses()

        logger.info(f"AddressManager initialized for markets: {', '.join(config.target_markets)}")

    def _is_valid_address(self, address: str) -> bool:
        """Check if address is a valid Ethereum address."""
        import re
        if not isinstance(address, str):
            return False

        # Remove any market prefix if present
        if ':' in address:
            address = address.split(':', 1)[1]

        # Check if it's a valid Ethereum address format
        return bool(re.match(r'^0x[a-fA-F0-9]{40}$', address.strip()))

    def _get_market_file(self, market: str) -> Path:
        """Get the path for a market-specific address file."""
        return self.data_dir / f"{market.lower()}_addresses.txt"

    def _load_all_market_addresses(self):
        """Load addresses from all market-specific files."""
        for market in self.config.target_markets:
            self._load_market_addresses(market)

        # Update general active addresses set
        self.active_addresses = set()
        for addresses in self.addresses_by_market.values():
            self.active_addresses.update(addresses)

    def _load_market_addresses(self, market: str):
        """Load addresses for a specific market from its file."""
        market_file = self._get_market_file(market)

        if not market_file.exists():
            logger.info(f"No existing addresses file for {market}, will create when needed")
            market_file.parent.mkdir(parents=True, exist_ok=True)
            return

        try:
            addresses_loaded = 0
            with open(market_file, 'r') as f:
                for line in f:
                    address = line.strip().lower()
                    if address and self._is_valid_address(address):
                        self.addresses_by_market[market].add(address)
                        self.active_addresses.add(address)
                        addresses_loaded += 1

            if addresses_loaded > 0:
                logger.info(f"Loaded {addresses_loaded} addresses for {market} from {market_file.name}")
        except Exception as e:
            logger.error(f"Error loading addresses for {market}: {e}")

    async def save_market_addresses(self, market: str):
        """Save addresses for a specific market to its file."""
        market_file = self._get_market_file(market)

        async with self.file_lock:
            try:
                # Ensure directory exists
                market_file.parent.mkdir(parents=True, exist_ok=True)

                # Write addresses to file (atomic write using temp file)
                tmp_path = Path(str(market_file) + '.tmp')

                with open(tmp_path, 'w') as f:
                    # Write header
                    f.write(f"# {market} addresses\n")
                    f.write(f"# Generated: {datetime.now().isoformat()}\n")
                    f.write(f"# Count: {len(self.addresses_by_market.get(market, set()))}\n\n")

                    # Write addresses sorted
                    for address in sorted(self.addresses_by_market.get(market, set())):
                        f.write(f"{address}\n")

                # Atomic replace
                tmp_path.replace(market_file)

                logger.debug(f"Saved {len(self.addresses_by_market.get(market, set()))} addresses for {market}")
                return True
            except Exception as e:
                logger.error(f"Error saving addresses for {market}: {e}")
                if tmp_path.exists():
                    tmp_path.unlink()
                return False

    async def save_all_market_addresses(self):
        """Save all market addresses to their respective files."""
        for market in self.addresses_by_market:
            await self.save_market_addresses(market)

    def _save_to_file_sync(self):
        """Synchronous version of save for use within threading lock."""
        # Create an event loop if needed or run in existing one
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Schedule the coroutine to run later
                asyncio.create_task(self.save_all_market_addresses())
            else:
                # Run it now
                loop.run_until_complete(self.save_all_market_addresses())
        except RuntimeError:
            # No event loop, create one
            asyncio.run(self.save_all_market_addresses())

    def update_from_snapshot(self, snapshot_addresses: Dict[str, Set[str]]) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        """
        Update addresses from new snapshot with dual-check logic.
        EXACT same logic as single-market version, just for multiple markets.

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

            # Update general active addresses set
            self.active_addresses = set()
            for addresses in self.addresses_by_market.values():
                self.active_addresses.update(addresses)

            # Save updated addresses to files
            if any(new_addresses.values()):
                self._save_to_file_sync()

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
                to_remove = closed_addrs & self.removal_candidates.get(market, set())

                if to_remove:
                    self.addresses_by_market[market] -= to_remove
                    self.removal_candidates[market] -= to_remove
                    self.active_addresses -= to_remove
                    total_removed += len(to_remove)

                    logger.info(f"Removed {len(to_remove)} {market} addresses (dual-check confirmed)")

            if total_removed > 0:
                # Save to market files after removals
                await self.save_all_market_addresses()

    async def sync_with_database(self, db_addresses_by_market: Dict[str, Set[str]]) -> Dict[str, Dict[str, Set[str]]]:
        """
        Sync addresses between files and database.

        Args:
            db_addresses_by_market: Addresses from database grouped by market

        Returns:
            Dictionary with 'to_add' and 'to_remove' for database sync
        """
        sync_actions = {'to_add': {}, 'to_remove': {}}

        with self.lock:
            for market in self.config.target_markets:
                file_addresses = self.addresses_by_market.get(market, set())
                db_addresses = db_addresses_by_market.get(market, set())

                # Addresses in file but not in DB (need to add to DB)
                to_add = file_addresses - db_addresses
                if to_add:
                    sync_actions['to_add'][market] = to_add
                    logger.info(f"{market}: {len(to_add)} addresses to add to database")

                # Addresses in DB but not in file (need to remove from DB)
                to_remove = db_addresses - file_addresses
                if to_remove:
                    sync_actions['to_remove'][market] = to_remove
                    logger.info(f"{market}: {len(to_remove)} addresses to remove from database")

        return sync_actions

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

    def get_market_addresses(self, market: str) -> Set[str]:
        """Get addresses for a specific market."""
        with self.lock:
            return self.addresses_by_market.get(market, set()).copy()

    def get_all_addresses(self) -> Set[str]:
        """Get all unique addresses across all markets."""
        with self.lock:
            return self.active_addresses.copy()

    def get_addresses_by_market(self) -> Dict[str, Set[str]]:
        """Get all addresses grouped by market."""
        with self.lock:
            return {market: addrs.copy() for market, addrs in self.addresses_by_market.items()}

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
            return sorted(list(self.active_addresses))

    def get_address_count(self) -> int:
        """Get total count of unique addresses being tracked."""
        with self.lock:
            return len(self.active_addresses)

    def get_stats(self) -> Dict[str, int]:
        """Get statistics about managed addresses."""

        with self.lock:
            stats = {
                'total_unique': len(self.active_addresses),
                'total_positions': sum(len(addrs) for addrs in self.addresses_by_market.values()),
            }

            for market in self.config.target_markets:
                stats[f'{market.lower()}_addresses'] = len(self.addresses_by_market.get(market, set()))
                stats[f'{market.lower()}_removal_candidates'] = len(self.removal_candidates.get(market, set()))

            return stats

    async def update_addresses(self, addresses_by_market: Dict[str, Set[str]]) -> int:
        """
        Update addresses from a source (like snapshot).

        Args:
            addresses_by_market: Dictionary of market -> addresses

        Returns:
            Total number of new addresses added
        """
        total_new = 0

        with self.lock:
            for market, addresses in addresses_by_market.items():
                if market not in self.addresses_by_market:
                    self.addresses_by_market[market] = set()

                # Find truly new addresses
                new_addresses = addresses - self.addresses_by_market[market]

                if new_addresses:
                    self.addresses_by_market[market].update(new_addresses)
                    self.active_addresses.update(new_addresses)
                    total_new += len(new_addresses)
                    logger.info(f"Added {len(new_addresses)} new addresses for {market}")

        if total_new > 0:
            await self.save_all_market_addresses()

        return total_new

    async def replace_addresses(self, addresses_by_market: Dict[str, Set[str]]) -> Dict[str, int]:
        """
        Replace all addresses with a new set (for snapshot seeding).
        Ensures we only track addresses with active positions.

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

            # Clear removal candidates since we've done a full replacement
            for market in self.removal_candidates:
                self.removal_candidates[market].clear()

            logger.info(f"Address replacement complete: {stats['total']} total addresses "
                       f"(+{stats['added']}/-{stats['removed']})")

        # Save to files
        await self.save_all_market_addresses()

        return stats