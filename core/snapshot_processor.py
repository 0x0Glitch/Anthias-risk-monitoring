import asyncio
import json
import msgpack
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, Tuple, Any, List
from dataclasses import dataclass
from decimal import Decimal

from config.constants import (
    ProcessingStatus,
    SYSTEM_ADDRESSES,
    FileConfig,
    MonitoringThresholds
)
from core.utils import is_ethereum_address

logger = logging.getLogger(__name__)

@dataclass
class SnapshotMetadata:
    """Metadata for a snapshot."""
    path: Path
    height: int
    date: str
    size: int
    hash: str
    processed_at: Optional[datetime] = None
    status: ProcessingStatus = ProcessingStatus.PENDING

class SnapshotProcessor:
    """
    Fixed snapshot processor using proper JSON parsing.
    """

    def __init__(self, config):
        self.config = config
        self.processing_lock = asyncio.Lock()
        self.state_file = config.data_dir / ".snapshot_state.json"
        self.processed_snapshots: Dict[str, SnapshotMetadata] = {}
        self.max_cache_size = FileConfig.MAX_SNAPSHOT_CACHE_SIZE
        self._load_state()

    def _load_state(self) -> None:
        """Load processing state from disk."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state_data = json.load(f)
                    for snap_id, snap_data in state_data.items():
                        if len(self.processed_snapshots) >= self.max_cache_size:
                            break
                        self.processed_snapshots[snap_id] = SnapshotMetadata(
                            path=Path(snap_data['path']),
                            height=snap_data['height'],
                            date=snap_data['date'],
                            size=snap_data['size'],
                            hash=snap_data['hash'],
                            processed_at=datetime.fromisoformat(snap_data['processed_at']) if snap_data.get('processed_at') else None,
                            status=ProcessingStatus(snap_data['status'])
                        )
                logger.info(f"Loaded {len(self.processed_snapshots)} snapshot states")
            except Exception as e:
                logger.warning(f"Could not load snapshot state: {e}")
                self.processed_snapshots = {}

    def _save_state(self) -> None:
        """Persist processing state to disk."""
        try:
            state_data = {}
            recent_snapshots = sorted(
                self.processed_snapshots.items(),
                key=lambda x: x[1].processed_at or datetime.min,
                reverse=True
            )[:self.max_cache_size]

            for snap_id, metadata in recent_snapshots:
                state_data[snap_id] = {
                    'path': str(metadata.path),
                    'height': metadata.height,
                    'date': metadata.date,
                    'size': metadata.size,
                    'hash': metadata.hash,
                    'processed_at': metadata.processed_at.isoformat() if metadata.processed_at else None,
                    'status': metadata.status.value
                }

            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp_file = self.state_file.with_suffix('.tmp')
            with open(tmp_file, 'w') as f:
                json.dump(state_data, f, indent=2)
            tmp_file.replace(self.state_file)
        except Exception as e:
            logger.error(f"Failed to save snapshot state: {e}")

    def _calculate_file_hash(self, path: Path) -> str:
        """Calculate SHA256 hash of file for deduplication."""
        sha256_hash = hashlib.sha256()
        try:
            with open(path, "rb") as f:
                # Read file in chunks to handle large files
                for chunk in iter(lambda: f.read(FileConfig.HASH_BLOCK_SIZE), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating hash for {path}: {e}")
            return ""

    def _parse_snapshot_info(self, snapshot_path: Path) -> Optional[SnapshotMetadata]:
        """Parse snapshot metadata from path and file info."""
        try:
            file_stat = snapshot_path.stat()
            file_hash = self._calculate_file_hash(snapshot_path)
            
            # Try to extract height and date from filename
            # Common patterns: height_YYYYMMDD_HHMMSS or YYYYMMDD_height
            filename = snapshot_path.name
            height = 0
            date = ""
            
            # Try different parsing strategies
            parts = filename.replace('.rmp', '').replace('.msgpack', '').split('_')
            for part in parts:
                if part.isdigit():
                    if len(part) == 8:  # Likely date YYYYMMDD
                        try:
                            datetime.strptime(part, '%Y%m%d')
                            date = part
                        except ValueError:
                            pass
                    elif len(part) > 8:  # Likely height (block number)
                        height = int(part)
            
            if not date:
                date = datetime.fromtimestamp(file_stat.st_mtime).strftime('%Y%m%d')
            
            return SnapshotMetadata(
                path=snapshot_path,
                height=height,
                date=date,
                size=file_stat.st_size,
                hash=file_hash
            )
        except Exception as e:
            logger.error(f"Error parsing snapshot info for {snapshot_path}: {e}")
            return None

    def _find_latest_snapshot(self) -> Optional[Path]:
        """Find the most recent unprocessed snapshot."""
        try:
            snapshots_dir = self.config.snapshots_dir
            if not snapshots_dir.exists():
                logger.warning(f"Snapshots directory does not exist: {snapshots_dir}")
                return None

            # Find all RMP files
            snapshot_files = []
            for pattern in ['*.rmp', '*.msgpack']:
                snapshot_files.extend(snapshots_dir.glob(pattern))

            if not snapshot_files:
                logger.warning(f"No snapshot files found in {snapshots_dir}")
                return None

            # Sort by modification time, newest first
            snapshot_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

            # Return first unprocessed snapshot
            for snapshot_file in snapshot_files:
                file_hash = self._calculate_file_hash(snapshot_file)
                if file_hash not in self.processed_snapshots:
                    return snapshot_file

            logger.info("All available snapshots have been processed")
            return None

        except Exception as e:
            logger.error(f"Error finding latest snapshot: {e}")
            return None

    async def process_latest_snapshot(self) -> Tuple[bool, Dict[str, Set[str]]]:
        """
        Main entry point for processing snapshots.
        Returns: (success, {market: {addresses}})
        """
        async with self.processing_lock:
            metadata = await self.find_latest_unprocessed_snapshot()
            if not metadata:
                logger.debug("No new snapshots to process")
                return False, {}

            logger.info(f"Processing snapshot: height={metadata.height} date={metadata.date}")

            try:
                # DIRECT RMP PROCESSING - no JSON conversion needed!
                positions = await self.extract_positions_from_rmp_direct(metadata.path, metadata)
                return True, positions

            except Exception as e:
                logger.error(f"Failed to process RMP snapshot {metadata.path}: {e}")
                metadata.status = ProcessingStatus.FAILED
                return False, {}

    async def _process_latest_snapshot_impl(self) -> Tuple[bool, Dict[str, Set[str]]]:
        """Implementation of snapshot processing."""
        try:
            # Find latest unprocessed snapshot
            snapshot_path = self._find_latest_snapshot()
            if not snapshot_path:
                logger.debug("No new snapshots to process")
                return False, {}

            # Parse snapshot metadata
            metadata = self._parse_snapshot_info(snapshot_path)
            if not metadata:
                logger.error(f"Could not parse metadata for {snapshot_path}")
                return False, {}

            logger.info(f"Processing snapshot: {snapshot_path.name} (height: {metadata.height}, size: {metadata.size:,} bytes)")

            # Mark as processing
            metadata.status = ProcessingStatus.PROCESSING
            self.processed_snapshots[metadata.hash] = metadata
            self._save_state()

            # Process the snapshot
            start_time = datetime.now()
            users_by_market = await self._extract_users_from_snapshot(snapshot_path)
            processing_time = datetime.now() - start_time

            if users_by_market:
                # Mark as successful
                metadata.status = ProcessingStatus.SUCCESS
                metadata.processed_at = datetime.now()
                
                total_users = sum(len(users) for users in users_by_market.values())
                logger.info(f"✓ Snapshot processed successfully in {processing_time.total_seconds():.2f}s")
                logger.info(f"  Found {total_users} unique users across {len(users_by_market)} markets")
                for market, users in users_by_market.items():
                    if users:
                        logger.info(f"  {market}: {len(users)} users")
            else:
                metadata.status = ProcessingStatus.FAILED
                logger.warning("No users found in snapshot")

            self.processed_snapshots[metadata.hash] = metadata
            self._save_state()

            return bool(users_by_market), users_by_market

        except Exception as e:
            logger.error(f"Error processing snapshot: {e}", exc_info=True)
            return False, {}

    async def _extract_users_from_snapshot(self, snapshot_path: Path) -> Dict[str, Set[str]]:
        """
        Extract users with positions from a snapshot.
        
        Returns:
            Dict[str, Set[str]]: users_by_market
        """
        users_by_market = {market: set() for market in self.config.target_markets}
        
        try:
            # Read and parse the msgpack file
            with open(snapshot_path, 'rb') as f:
                data = msgpack.unpack(f, strict_map_key=False)
            
            logger.info(f"Loaded snapshot data, analyzing structure...")
            
            # Navigate the data structure to find position information
            positions_found = 0
            addresses_found = set()
            
            users_data = self._extract_users_data(data)
            
            for user_address, user_info in users_data.items():
                # Validate address format
                if not is_ethereum_address(user_address):
                    continue
                
                # Skip system addresses
                if user_address.lower() in SYSTEM_ADDRESSES:
                    continue
                
                # Extract positions for this user
                positions = self._extract_positions_from_user(user_info)
                
                for market, position_data in positions.items():
                    if market in self.config.target_markets:
                        position_size = position_data.get('size', 0)
                        position_value = position_data.get('value', 0)
                        
                        # Filter by minimum position size
                        if (abs(position_size) > 0 and 
                            position_value >= self.config.min_position_size_usd):
                            users_by_market[market].add(user_address)
                            addresses_found.add(user_address)
                            positions_found += 1
            
            logger.info(f"Extracted {positions_found} positions from {len(addresses_found)} addresses")
            
            return users_by_market

        except Exception as e:
            logger.error(f"Error extracting users from snapshot {snapshot_path}: {e}")
            return {market: set() for market in self.config.target_markets}

    def _extract_users_data(self, data: Any) -> Dict[str, Any]:
        """
        Extract user data from the snapshot structure.
        This handles the complex nested structure of RMP snapshots.
        """
        users_data = {}
        
        try:
            # RMP snapshots have a complex structure - try different approaches
            if isinstance(data, dict):
                # Try direct user mapping
                if 'users' in data:
                    users_data.update(data['users'])
                
                # Try state mapping
                if 'state' in data and isinstance(data['state'], dict):
                    state = data['state']
                    if 'users' in state:
                        users_data.update(state['users'])
                    
                    # Sometimes users are under different keys
                    for key in ['accounts', 'positions', 'user_states']:
                        if key in state and isinstance(state[key], dict):
                            users_data.update(state[key])
                
                # Try top-level address mappings
                for key, value in data.items():
                    if isinstance(key, str) and is_ethereum_address(key):
                        users_data[key] = value
                    elif isinstance(value, dict):
                        # Recursively search for user data
                        nested_users = self._extract_users_data(value)
                        users_data.update(nested_users)
            
            elif isinstance(data, list):
                # If data is a list, check each item
                for item in data:
                    if isinstance(item, dict):
                        nested_users = self._extract_users_data(item)
                        users_data.update(nested_users)
            
        except Exception as e:
            logger.debug(f"Error extracting users data: {e}")
        
        return users_data

    def _extract_positions_from_user(self, user_info: Any) -> Dict[str, Dict]:
        """
        Extract position information for a specific user.
        """
        positions = {}
        
        try:
            if not isinstance(user_info, dict):
                return positions
            
            # Look for position data in various locations
            position_sources = [
                user_info.get('positions', {}),
                user_info.get('assetPositions', {}),
                user_info.get('perp_positions', {}),
                user_info
            ]
            
            for source in position_sources:
                if not isinstance(source, dict):
                    continue
                
                for key, pos_data in source.items():
                    if not isinstance(pos_data, dict):
                        continue
                    
                    # Try to determine market/coin
                    market = None
                    if 'coin' in pos_data:
                        market = str(pos_data['coin']).upper()
                    elif 'market' in pos_data:
                        market = str(pos_data['market']).upper()
                    elif 'symbol' in pos_data:
                        market = str(pos_data['symbol']).upper()
                    elif isinstance(key, str) and key.upper() in self.config.target_markets:
                        market = key.upper()
                    
                    if not market or market not in self.config.target_markets:
                        continue
                    
                    # Extract position size and value
                    position_size = self._safe_float(pos_data.get('szi', 0))
                    if position_size == 0:
                        position_size = self._safe_float(pos_data.get('size', 0))
                    
                    # Calculate position value
                    position_value = self._safe_float(pos_data.get('positionValue', 0))
                    if position_value == 0:
                        # Try to calculate from size and price
                        entry_price = self._safe_float(pos_data.get('entryPx', 0))
                        mark_price = self._safe_float(pos_data.get('markPx', 0))
                        price = mark_price or entry_price
                        if price > 0:
                            position_value = abs(position_size) * price
                    
                    if abs(position_size) > 0:
                        positions[market] = {
                            'size': position_size,
                            'value': position_value,
                            'raw_data': pos_data
                        }
            
        except Exception as e:
            logger.debug(f"Error extracting positions from user: {e}")
        
        return positions

    def _safe_float(self, value: Any) -> float:
        """Safely convert value to float."""
        try:
            if value is None:
                return 0.0
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                return float(value) if value else 0.0
            if isinstance(value, Decimal):
                return float(value)
            return 0.0
        except (ValueError, TypeError):
            return 0.0

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        stats = {
            'total_processed': len(self.processed_snapshots),
            'successful': len([s for s in self.processed_snapshots.values() if s.status == ProcessingStatus.SUCCESS]),
            'failed': len([s for s in self.processed_snapshots.values() if s.status == ProcessingStatus.FAILED]),
            'pending': len([s for s in self.processed_snapshots.values() if s.status == ProcessingStatus.PENDING])
        }
        
        if self.processed_snapshots:
            latest = max(self.processed_snapshots.values(), key=lambda x: x.processed_at or datetime.min)
            stats['latest_processed'] = {
                'path': str(latest.path),
                'height': latest.height,
                'processed_at': latest.processed_at.isoformat() if latest.processed_at else None,
                'status': latest.status.value
            }
        
        return stats

    def cleanup_old_snapshots(self, keep_count: int = None) -> None:
        """Clean up old processed snapshots from state."""
        if keep_count is None:
            keep_count = FileConfig.SNAPSHOT_RETENTION_COUNT
        
        if len(self.processed_snapshots) <= keep_count:
            return
        
        # Sort by processed time, keep most recent
        sorted_snapshots = sorted(
            self.processed_snapshots.items(),
            key=lambda x: x[1].processed_at or datetime.min,
            reverse=True
        )
        
        # Keep only the most recent ones
        self.processed_snapshots = dict(sorted_snapshots[:keep_count])
        self._save_state()
        
        logger.info(f"Cleaned up old snapshots, kept {len(self.processed_snapshots)} most recent")

    async def find_latest_unprocessed_snapshot(self) -> Optional[SnapshotMetadata]:
        """Find the latest unprocessed RMP snapshot."""
        if not self.config.rmp_base_path.exists():
            logger.warning(f"RMP base path does not exist: {self.config.rmp_base_path}")
            return None

        try:
            candidates = []
            for date_dir in sorted(self.config.rmp_base_path.iterdir(), reverse=True):
                if not date_dir.is_dir() or not date_dir.name.replace('-', '').isdigit():
                    continue

                for rmp_file in sorted(date_dir.glob("*.rmp"), reverse=True):
                    try:
                        if rmp_file.stat().st_size < 1000:
                            continue

                        height = int(rmp_file.stem)
                        file_hash = self._calculate_file_hash(rmp_file)
                        metadata = SnapshotMetadata(
                            path=rmp_file,
                            height=height,
                            date=date_dir.name,
                            size=rmp_file.stat().st_size,
                            hash=file_hash
                        )

                        if file_hash in self.processed_snapshots:
                            existing = self.processed_snapshots[file_hash]
                            if existing.status == ProcessingStatus.SUCCESS:
                                continue

                        candidates.append(metadata)
                        if len(candidates) == 1:
                            return metadata

                    except (ValueError, OSError) as e:
                        logger.debug(f"Invalid RMP file {rmp_file}: {e}")
                        continue

                if candidates:
                    break

            return candidates[0] if candidates else None

        except Exception as e:
            logger.error(f"Error scanning for snapshots: {e}", exc_info=True)
            return None

    async def extract_positions_from_rmp_direct(
        self,
        rmp_path: Path,
        metadata: SnapshotMetadata
    ) -> Dict[str, Set[str]]:
        """
        DIRECT RMP PROCESSING: Parse MessagePack directly without JSON conversion.
        Uses the same proven logic as extract_link_from_rmp_direct.py
        """

        result: Dict[str, Set[str]] = {
            market: set() for market in self.config.target_markets
        }

        try:
            logger.info(f"🔄 DIRECT RMP PARSING from {rmp_path}...")
            logger.info(f"File size: {rmp_path.stat().st_size / (1024*1024):.1f}MB")

            # Load RMP data directly with msgpack
            with open(rmp_path, 'rb') as f:
                data = msgpack.unpack(f, raw=False, strict_map_key=False)

            logger.info("✅ Successfully loaded RMP data into memory")

            # Derive asset indices and mark prices from this snapshot's universe (NEVER hardcode!)
            market_to_index, market_to_price = self._derive_asset_indices(data)

            if not market_to_index:
                logger.error("No target markets found in RMP universe")
                return result

            logger.info(f"✓ Derived indices for {len(market_to_index)} markets")
            logger.info(f"✓ Extracted prices for {len(market_to_price)} markets")

            # System addresses to filter out
            system_addresses = SYSTEM_ADDRESSES

            # Extract positions
            total_positions_found = 0
            processed_count = 0

            # Navigate to user data
            if 'exchange' in data and 'perp_dexs' in data['exchange']:
                for dex in data['exchange']['perp_dexs']:
                    if 'clearinghouse' in dex and 'user_to_state' in dex['clearinghouse']:
                        user_to_state = dex['clearinghouse']['user_to_state']
                        
                        for address, user_data in user_to_state.items():
                            if not is_ethereum_address(address) or address.lower() in system_addresses:
                                continue

                            positions_found = self._process_user_positions(
                                address.lower(), user_data, market_to_index, market_to_price, result
                            )
                            total_positions_found += positions_found
                            processed_count += 1

                            if processed_count % 1000 == 0:
                                logger.info(f"Processed {processed_count} users, found {total_positions_found} positions...")

            # Log results
            total_unique_addresses = sum(len(addrs) for addrs in result.values())
            logger.info(f"✅ EXTRACTION COMPLETE")
            logger.info(f"✓ Found {total_positions_found} active positions")
            logger.info(f"✓ Found {total_unique_addresses} unique addresses with positions")

            for market, addresses in result.items():
                if addresses:
                    logger.info(f"  {market}: {len(addresses)} addresses with active positions")

            # Mark as successful
            metadata.status = ProcessingStatus.SUCCESS
            metadata.processed_at = datetime.now()
            self.processed_snapshots[metadata.hash] = metadata
            self._save_state()

            return result

        except Exception as e:
            logger.error(f"Error in RMP extraction: {e}", exc_info=True)
            metadata.status = ProcessingStatus.FAILED
            return result

    def _derive_asset_indices(self, data: Dict) -> Tuple[Dict[str, int], Dict[str, float]]:
        """
        Derive asset indices and mark prices from meta.universe in the snapshot.
        NEVER hardcode indices - they can change between snapshots!
        """
        market_to_index = {}
        market_to_price = {}

        if 'exchange' in data and 'perp_dexs' in data['exchange']:
            for dex in data['exchange']['perp_dexs']:
                if 'clearinghouse' in dex and 'meta' in dex['clearinghouse']:
                    if 'universe' in dex['clearinghouse']['meta']:
                        universe = dex['clearinghouse']['meta']['universe']
                        logger.info(f"Found universe with {len(universe)} assets")

                        for i, asset in enumerate(universe):
                            name = asset.get('name', '').upper()
                            if i < 10:  # Log first 10 for debugging
                                logger.info(f"Asset {i}: {name}")

                            if name in self.config.target_markets:
                                market_to_index[name] = i
                                logger.info(f"✓ Found target market {name} at index {i}")

                        # Extract mark prices from asset_ctxs
                        if 'asset_ctxs' in dex['clearinghouse']['meta']:
                            asset_ctxs = dex['clearinghouse']['meta']['asset_ctxs']
                            if isinstance(asset_ctxs, list) and len(asset_ctxs) == len(universe):
                                for market, index in market_to_index.items():
                                    if index < len(asset_ctxs):
                                        ctx = asset_ctxs[index]
                                        if isinstance(ctx, dict) and 'mark_px' in ctx:
                                            try:
                                                price = float(ctx['mark_px'])
                                                market_to_price[market] = price
                                                logger.info(f"✓ Extracted mark price for {market}: ${price:,.2f}")
                                            except (ValueError, TypeError):
                                                logger.warning(f"Invalid mark_px for {market}: {ctx.get('mark_px')}")
                                                market_to_price[market] = 1.0  # Fallback

                        break  # Use first dex with universe

        return market_to_index, market_to_price

    def _calculate_position_value_from_snapshot(
        self,
        pos_data: Dict,
        position_size: float,
        mark_price: float
    ) -> float:
        """
        Calculate position value in USD from snapshot data using same logic as live API.
        This ensures consistent qualification between snapshot extraction and live updates.
        """
        try:
            # Try to get position value directly (if available in snapshot)
            position_value = pos_data.get('positionValue') or pos_data.get('position_value') or pos_data.get('v')
            if position_value:
                return float(position_value)

            # Try to get entry price for calculation
            entry_px = (pos_data.get('entryPx') or pos_data.get('entry_px') or
                       pos_data.get('e') or pos_data.get('ep'))

            if entry_px and float(entry_px) > 0:
                return abs(position_size * float(entry_px))

            # Fallback: use mark price
            if mark_price > 0:
                return abs(position_size * mark_price)

            # Last resort: assume position_size represents USD value
            return abs(position_size)

        except (ValueError, TypeError):
            # If all else fails, assume position_size is USD value
            return abs(position_size)

    def extract_position_size(self, pos_data: Dict) -> float:
        """Extract position size supporting both new (szi) and old (s) formats."""
        # NEW FORMAT: 'szi' field
        if 'szi' in pos_data:
            value = pos_data['szi']
            if isinstance(value, (int, float, str)):
                try:
                    return float(Decimal(str(value)))
                except (ValueError, TypeError):
                    pass

        # OLD FORMAT: 's' field
        if 's' in pos_data:
            value = pos_data['s']
            if isinstance(value, (int, float, str)):
                try:
                    return float(Decimal(str(value)))
                except (ValueError, TypeError):
                    pass

        # Fallback fields
        for field in ['sz', 'size', 'amount']:
            if field in pos_data:
                value = pos_data[field]
                if isinstance(value, (int, float, str)):
                    try:
                        return float(Decimal(str(value)))
                    except (ValueError, TypeError):
                        pass

        return 0.0

    async def _cleanup_old_json_files(self) -> None:
        """Clean up old JSON files to save disk space."""
        try:
            json_files = sorted(
                self.config.data_dir.glob("snapshot_*.json"),
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )

            if len(json_files) > self.config.snapshot_retention_count:
                for old_file in json_files[self.config.snapshot_retention_count:]:
                    try:
                        old_file.unlink()
                        logger.debug(f"Deleted old snapshot: {old_file.name}")
                    except Exception as e:
                        logger.warning(f"Could not delete {old_file}: {e}")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
