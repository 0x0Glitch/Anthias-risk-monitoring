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
                for byte_block in iter(lambda: f.read(FileConfig.HASH_BLOCK_SIZE), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()[:16]
        except Exception as e:
            logger.error(f"Failed to hash file {path}: {e}")
            return str(path)

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

    async def convert_rmp_to_json(self, metadata: SnapshotMetadata) -> Optional[Path]:
        """Convert RMP to JSON with proper error handling and retries."""
        if not self.config.node_binary_path.exists():
            logger.error(f"hl-node binary not found at {self.config.node_binary_path}")
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = self.config.data_dir / f"snapshot_{metadata.height}_{metadata.date}_{timestamp}.json"

        metadata.status = ProcessingStatus.PROCESSING
        self.processed_snapshots[metadata.hash] = metadata

        cmd = [
            str(self.config.node_binary_path),
            "--chain", self.config.chain_type,
            "translate-abci-state",
            str(metadata.path),
            str(json_path)
        ]

        logger.info(f"Converting RMP (height: {metadata.height}, size: {metadata.size/1024/1024:.1f}MB)")

        for attempt in range(3):
            try:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    limit=1024*1024*10
                )

                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=300
                )

                if process.returncode != 0:
                    error_msg = stderr.decode('utf-8', errors='ignore')[:500]
                    logger.error(f"RMP conversion failed (attempt {attempt+1}): {error_msg}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    metadata.status = ProcessingStatus.FAILED
                    return None

                if not json_path.exists() or json_path.stat().st_size < 1000:
                    logger.error(f"Invalid JSON output: {json_path}")
                    if json_path.exists():
                        json_path.unlink()
                    metadata.status = ProcessingStatus.FAILED
                    return None

                json_size_mb = json_path.stat().st_size / 1024 / 1024
                logger.info(f"Successfully converted to JSON: {json_path.name} ({json_size_mb:.1f}MB)")

                asyncio.create_task(self._cleanup_old_json_files())
                return json_path

            except asyncio.TimeoutError:
                logger.error(f"RMP conversion timeout (attempt {attempt+1})")
                if attempt < 2:
                    await asyncio.sleep(5)
                    continue
            except Exception as e:
                logger.error(f"RMP conversion error: {e}", exc_info=True)
                if attempt < 2:
                    await asyncio.sleep(2)
                    continue

        metadata.status = ProcessingStatus.FAILED
        return None

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
                logger.error("No target markets found in universe")
                return result

            logger.info(f"✓ Derived indices for {len(market_to_index)} markets")
            logger.info(f"✓ Extracted prices for {len(market_to_price)} markets")

            # Parse clearinghouse state using the SAME logic as working direct parser
            total_positions_found = 0

            if 'exchange' in data and 'perp_dexs' in data['exchange']:
                for dex_idx, dex in enumerate(data['exchange']['perp_dexs']):
                    if 'clearinghouse' not in dex:
                        continue

                    clearinghouse = dex['clearinghouse']

                    # NEW SCHEMA: user_states (dict format)
                    if 'user_states' in clearinghouse and isinstance(clearinghouse['user_states'], dict):
                        logger.info(f"Processing {len(clearinghouse['user_states'])} user_states entries")

                        # FIXED: Check if there's a user_to_state mapping (actual user data)
                        if 'user_to_state' in clearinghouse['user_states']:
                            user_to_state = clearinghouse['user_states']['user_to_state']
                            logger.info(f"Found user_to_state with {len(user_to_state)} users")

                            # user_to_state can be either dict or list of [address, user_data] pairs
                            if isinstance(user_to_state, dict):
                                user_items = user_to_state.items()
                            elif isinstance(user_to_state, list):
                                user_items = user_to_state
                            else:
                                logger.warning(f"Unexpected user_to_state type: {type(user_to_state)}")
                                user_items = []

                            for item in user_items:
                                if isinstance(item, (list, tuple)) and len(item) >= 2:
                                    address, user_data = item[0], item[1]
                                elif isinstance(user_to_state, dict):
                                    address, user_data = item  # This is a tuple from .items()
                                else:
                                    continue

                                if not is_ethereum_address(address):
                                    continue

                                address_lower = address.lower()
                                if address_lower in SYSTEM_ADDRESSES:
                                    continue

                                # Process positions using the SAME logic as working direct parser
                                positions_found = self._process_user_positions_direct(
                                    address_lower, user_data, market_to_index, market_to_price, result
                                )
                                total_positions_found += positions_found

                    # LEGACY SCHEMA: books (list format) - same as working direct parser
                    elif 'books' in clearinghouse and isinstance(clearinghouse['books'], list):
                        logger.info(f"Processing {len(clearinghouse['books'])} book entries")

                        for book_entry in clearinghouse['books']:
                            if not (isinstance(book_entry, list) and len(book_entry) >= 2):
                                continue

                            address, user_data = book_entry[0], book_entry[1]

                            if not is_ethereum_address(address):
                                continue

                            address_lower = address.lower()
                            if address_lower in SYSTEM_ADDRESSES:
                                continue

                            # Process legacy positions
                            positions_found = self._process_user_positions_direct(
                                address_lower, user_data, market_to_index, market_to_price, result
                            )
                            total_positions_found += positions_found

            # INVARIANT CHECK: Ensure we didn't over-extract
            total_unique_addresses = sum(len(addrs) for addrs in result.values())

            # This should now be true: unique addresses ≤ total positions
            if total_unique_addresses > total_positions_found:
                logger.error(f"INVARIANT VIOLATION: {total_unique_addresses} addresses > {total_positions_found} positions")
            else:
                logger.info(f"✅ INVARIANT SATISFIED: {total_unique_addresses} addresses ≤ {total_positions_found} positions")

            # Log results
            logger.info(f"\n📈 EXTRACTION COMPLETE")
            logger.info(f"✓ Found {total_positions_found} active positions")
            logger.info(f"✓ Found {total_unique_addresses} unique addresses with positions")
            logger.info(f"📊 Addresses distributed across {len(result)} markets")

            # Mark as successful
            metadata.status = ProcessingStatus.SUCCESS
            metadata.processed_at = datetime.now()
            self.processed_snapshots[metadata.hash] = metadata

            self._save_state()

            return result

        except Exception as e:
            logger.error(f"Error in direct RMP extraction: {e}", exc_info=True)
            metadata.status = ProcessingStatus.FAILED

        return result

    def _process_user_positions_direct(
        self,
        address: str,
        user_data: Dict,
        market_to_index: Dict[str, int],
        market_to_price: Dict[str, float],
        result: Dict[str, Set[str]]
    ) -> int:
        """
        Process positions for a single user using DIRECT RMP logic.
        Uses the same exact logic as extract_link_from_rmp_direct.py
        """
        positions_found = 0

        try:
            # NEW FORMAT: assetPositions with szi (same as direct parser)
            if 'asset_positions' in user_data and isinstance(user_data['asset_positions'], list):
                for asset_pos in user_data['asset_positions']:
                    if isinstance(asset_pos, dict) and 'position' in asset_pos:
                        position = asset_pos['position']
                        if isinstance(position, dict):
                            coin = position.get('coin', '').upper()

                            if coin in market_to_index:
                                szi_str = position.get('szi', '0')
                                try:
                                    szi = Decimal(str(szi_str))
                                    if szi != 0:
                                        position_value_usd = self._calculate_position_value_from_snapshot(
                                            position, float(szi), market_to_price.get(coin, 1.0)
                                        )

                                        if position_value_usd >= self.config.min_position_size_usd:
                                            result[coin].add(address)
                                            positions_found += 1
                                            logger.debug(f"✓ {coin} position: {address} size={szi} value=${position_value_usd:.2f}")
                                except (ValueError, TypeError):
                                    continue

            # LEGACY FORMAT: p.p structure (same as direct parser)
            elif 'p' in user_data and isinstance(user_data['p'], dict) and 'p' in user_data['p']:
                positions_list = user_data['p']['p']
                if isinstance(positions_list, list):
                    for pos_item in positions_list:
                        if not (isinstance(pos_item, list) and len(pos_item) >= 2):
                            continue

                        asset_idx, pos_data = pos_item[0], pos_item[1]

                        # Find which market this index corresponds to
                        target_market = None
                        for market, index in market_to_index.items():
                            if asset_idx == index:
                                target_market = market
                                break

                        if target_market and isinstance(pos_data, dict):
                            size_value = pos_data.get('s') or pos_data.get('sz', '0')
                            try:
                                size = Decimal(str(size_value))
                                if size != 0:
                                    position_value_usd = self._calculate_position_value_from_snapshot(
                                        pos_data, float(size), market_to_price.get(target_market, 1.0)
                                    )

                                    if position_value_usd >= self.config.min_position_size_usd:
                                        result[target_market].add(address)
                                        positions_found += 1
                                        logger.debug(f"✓ {target_market} legacy position: {address} size={size} value=${position_value_usd:.2f}")
                            except (ValueError, TypeError):
                                continue

        except Exception as e:
            logger.debug(f"Error processing positions for {address}: {e}")

        return positions_found

    async def _extract_metadata_chunked(self, json_path: Path) -> Tuple[Dict[str, int], Dict[str, float]]:
        """
        Extract metadata (universe, prices) by reading file in chunks.
        Only keeps the metadata in memory, not the entire JSON structure.
        """
        market_to_index = {}
        market_to_price = {}

        try:
            logger.info("📖 Reading metadata from JSON file in chunks...")

            # Large chunk size to capture complete arrays efficiently
            chunk_size = 500 * 1024 * 1024  # 500MB chunks for large universe
            buffer = ""
            universe_found = False

            with open(json_path, 'r', encoding='utf-8') as f:
                while not universe_found:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break

                    buffer += chunk

                    # Look for universe section
                    if '"universe":[' in buffer:
                        logger.info("🎯 Found universe section, extracting...")

                        # Continue reading until we have the complete universe array
                        start_idx = buffer.find('"universe":[') + 11  # After "universe":[

                        # Make sure we have enough data by continuing to read
                        while True:
                            # Count brackets properly, handling nested structures
                            bracket_count = 1
                            in_string = False
                            escape_next = False
                            end_idx = start_idx

                            while bracket_count > 0 and end_idx < len(buffer):
                                char = buffer[end_idx]

                                if escape_next:
                                    escape_next = False
                                elif char == '\\':
                                    escape_next = True
                                elif char == '"' and not escape_next:
                                    in_string = not in_string
                                elif not in_string:
                                    if char == '[':
                                        bracket_count += 1
                                    elif char == ']':
                                        bracket_count -= 1

                                end_idx += 1

                            if bracket_count == 0:
                                # We found the complete universe array
                                try:
                                    universe_json = '[' + buffer[start_idx:end_idx-1] + ']'
                                    universe = json.loads(universe_json)

                                    logger.info(f"Found universe with {len(universe)} assets")

                                    # Log ALL assets to debug LINK not being found
                                    for i, asset in enumerate(universe):
                                        name = asset.get('name', '').upper()
                                        logger.info(f"Asset {i}: {name}")
                                        if name in self.config.target_markets:
                                            market_to_index[name] = i
                                            logger.info(f"✓✓✓ Found target market {name} at index {i}")

                                    universe_found = True
                                    break
                                except json.JSONDecodeError as e:
                                    logger.error(f"JSON decode error in universe extraction: {e}")
                                    # Try to read more data

                            # Need more data, read another chunk
                            more_chunk = f.read(chunk_size)
                            if not more_chunk:
                                logger.warning("Reached EOF while looking for universe end")
                                break
                            buffer += more_chunk

                    # If we haven't found universe yet, keep reading
                    if not universe_found and len(buffer) > chunk_size * 3:
                        # Only keep the last portion to prevent infinite growth
                        buffer = buffer[-chunk_size:]

                # Now look for asset_ctxs to get prices
                if market_to_index:
                    f.seek(0)  # Reset file position
                    buffer = ""
                    asset_ctxs_found = False

                    while not asset_ctxs_found:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break

                        buffer += chunk

                        if '"asset_ctxs":[' in buffer:
                            logger.info("🎯 Found asset_ctxs section, extracting prices...")
                            try:
                                start = buffer.find('"asset_ctxs":[') + 13
                                bracket_count = 1
                                end = start

                                while bracket_count > 0 and end < len(buffer):
                                    if buffer[end] == '[':
                                        bracket_count += 1
                                    elif buffer[end] == ']':
                                        bracket_count -= 1
                                    end += 1

                                if bracket_count == 0:
                                    asset_ctxs_json = '[' + buffer[start:end-1] + ']'
                                    asset_ctxs = json.loads(asset_ctxs_json)

                                    # Extract mark prices
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

                                    asset_ctxs_found = True
                                    break
                            except json.JSONDecodeError as e:
                                logger.debug(f"JSON decode error in asset_ctxs extraction: {e}")

                        # Keep only the end of buffer
                        if len(buffer) > chunk_size * 2:
                            buffer = buffer[-chunk_size:]

        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")

        return market_to_index, market_to_price

    async def _extract_positions_chunked(
        self,
        json_path: Path,
        market_to_index: Dict[str, int],
        market_to_price: Dict[str, float],
        system_addresses: Set[str],
        result: Dict[str, Set[str]]
    ) -> int:
        """
        Extract positions by processing the JSON file in chunks.
        Looks for user_to_state or books sections and processes users incrementally.
        """
        total_positions_found = 0
        processed_count = 0

        try:
            chunk_size = 500 * 1024 * 1024  # 500MB chunks
            buffer = ""
            in_user_section = False
            user_buffer = ""
            brace_count = 0

            logger.info("🔄 Streaming positions from JSON file in chunks...")

            with open(json_path, 'r', encoding='utf-8') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break

                    buffer += chunk

                    # Look for user_to_state section
                    if not in_user_section and '"user_to_state":{' in buffer:
                        logger.info("🎯 Found user_to_state section, processing users...")
                        in_user_section = True
                        start_idx = buffer.find('"user_to_state":{') + len('"user_to_state":{')
                        buffer = '{' + buffer[start_idx:]  # Keep opening brace
                        brace_count = 1  # Start with 1 for the opening brace

                    if in_user_section:
                        # Process character by character to find complete user objects
                        i = 0
                        while i < len(buffer):
                            char = buffer[i]
                            user_buffer += char

                            if char == '{':
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1

                                # When brace_count hits 0, we have a complete user object
                                if brace_count == 0 and len(user_buffer.strip()) > 10:
                                    # Try to extract and process this user
                                    try:
                                        # Find the user address (key) and data
                                        user_entry = user_buffer.strip().rstrip(',')
                                        if '":' in user_entry:
                                            addr_end = user_entry.find('":')
                                            if addr_end > 0:
                                                address = user_entry[1:addr_end]  # Remove quotes
                                                user_data_json = user_entry[addr_end + 2:]  # After ":

                                                if is_ethereum_address(address):
                                                    user_data = json.loads(user_data_json)
                                                    address_lower = address.lower()

                                                    if address_lower not in system_addresses:
                                                        positions_found = self._process_user_positions(
                                                            address_lower, user_data, market_to_index, market_to_price, result
                                                        )
                                                        total_positions_found += positions_found
                                                        processed_count += 1

                                                        if processed_count % 1000 == 0:
                                                            logger.info(f"Processed {processed_count} users, found {total_positions_found} positions...")
                                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                                        logger.debug(f"Error processing user entry: {e}")

                                    user_buffer = ""  # Reset for next user

                            i += 1

                        # Keep the processed part, remove what we've analyzed
                        buffer = buffer[i:]

                    # Prevent buffer from growing too large
                    if not in_user_section and len(buffer) > chunk_size * 3:
                        buffer = buffer[-chunk_size:]  # Keep last chunk for boundaries

        except Exception as e:
            logger.error(f"Error in chunked position extraction: {e}")

        logger.info(f"📊 Processed {processed_count} users total")
        return total_positions_found

    def _process_user_positions(
        self,
        address: str,
        user_data: Dict,
        market_to_index: Dict[str, int],
        market_to_price: Dict[str, float],
        result: Dict[str, Set[str]]
    ) -> int:
        """
        Process positions for a single user, handling both new and legacy formats.
        Returns the number of positions found for this user.
        """
        positions_found = 0

        try:
            # NEW FORMAT: assetPositions with szi
            if 'asset_positions' in user_data:
                for asset_pos in user_data['asset_positions']:
                    position = asset_pos.get('position', {})
                    coin = position.get('coin', '').upper()

                    if coin in market_to_index:
                        szi_str = position.get('szi', '0')
                        try:
                            szi = Decimal(str(szi_str))
                            if szi != 0:
                                position_value_usd = self._calculate_position_value_from_snapshot(
                                    position, float(szi), market_to_price.get(coin, 1.0)
                                )

                                if position_value_usd >= self.config.min_position_size_usd:
                                    result[coin].add(address)
                                    positions_found += 1
                        except (ValueError, TypeError):
                            continue

            # LEGACY FORMAT: p.p structure
            elif 'p' in user_data and isinstance(user_data['p'], dict) and 'p' in user_data['p']:
                positions_list = user_data['p']['p']
                if isinstance(positions_list, list):
                    for pos_item in positions_list:
                        if not (isinstance(pos_item, list) and len(pos_item) >= 2):
                            continue

                        asset_idx, pos_data = pos_item[0], pos_item[1]

                        # Find which market this index corresponds to
                        target_market = None
                        for market, index in market_to_index.items():
                            if asset_idx == index:
                                target_market = market
                                break

                        if target_market and isinstance(pos_data, dict):
                            size_value = pos_data.get('s') or pos_data.get('sz', '0')
                            try:
                                size = Decimal(str(size_value))
                                if size != 0:
                                    position_value_usd = self._calculate_position_value_from_snapshot(
                                        pos_data, float(size), market_to_price.get(target_market, 1.0)
                                    )

                                    if position_value_usd >= self.config.min_position_size_usd:
                                        result[target_market].add(address)
                                        positions_found += 1
                            except (ValueError, TypeError):
                                continue

        except Exception as e:
            logger.debug(f"Error processing positions for {address}: {e}")

        return positions_found

    async def extract_positions_from_json(
        self,
        json_path: Path,
        metadata: SnapshotMetadata
    ) -> Dict[str, Set[str]]:
        """
        FIXED VERSION: Use proper JSON parsing instead of regex.
        Ensures unique addresses = users with active positions.
        """

        result: Dict[str, Set[str]] = {
            market: set() for market in self.config.target_markets
        }

        try:
            logger.info(f"🔄 CHUNKED STREAMING PARSE from {json_path}...")
            logger.info(f"File size: {json_path.stat().st_size / (1024*1024):.1f}MB")

            # Extract metadata using chunked reading to avoid memory issues
            market_to_index, market_to_price = await self._extract_metadata_chunked(json_path)

            if not market_to_index:
                logger.error("No target markets found in universe")
                return result

            logger.info(f"✓ Derived indices for {len(market_to_index)} markets")
            logger.info(f"✓ Extracted prices for {len(market_to_price)} markets")

            # System addresses to filter out
            system_addresses = {
                '0x0000000000000000000000000000000000000000',
                '0x0000000000000000000000000000000000000001',
                '0x000000000000000000000000000000000000dead',
                '0xffffffffffffffffffffffffffffffffffffffff'
            }

            # Parse positions using chunked streaming to avoid memory issues
            total_positions_found = await self._extract_positions_chunked(
                json_path, market_to_index, market_to_price, system_addresses, result
            )

            # INVARIANT CHECK: Ensure we didn't over-extract
            total_unique_addresses = sum(len(addrs) for addrs in result.values())

            # This should now be true: unique addresses ≤ total positions
            if total_unique_addresses > total_positions_found:
                logger.error(f"INVARIANT VIOLATION: {total_unique_addresses} addresses > {total_positions_found} positions")
                # This indicates a bug in the extraction logic
            else:
                logger.info(f"✅ INVARIANT SATISFIED: {total_unique_addresses} addresses ≤ {total_positions_found} positions")

            # Log results
            logger.info(f"\n📈 EXTRACTION COMPLETE")
            logger.info(f"✓ Found {total_positions_found} active positions")
            logger.info(f"✓ Found {total_unique_addresses} unique addresses with positions")

            for market, addresses in result.items():
                if addresses:
                    logger.info(f"  {market}: {len(addresses)} addresses with active positions")

            # Write all addresses to file for debugging
            all_unique_addresses = set()
            for market, addresses in result.items():
                all_unique_addresses.update(addresses)

            lol_file = self.config.data_dir / "active_addresses_found.txt"
            try:
                with open(lol_file, 'w') as f:
                    f.write(f"# Active addresses extracted from snapshot height {metadata.height}\n")
                    f.write(f"# Total positions: {total_positions_found}\n")
                    f.write(f"# Unique addresses: {len(all_unique_addresses)}\n")
                    f.write(f"# Extraction time: {datetime.now().isoformat()}\n\n")

                    for market in sorted(result.keys()):
                        addresses = result[market]
                        if addresses:
                            f.write(f"# {market} ({len(addresses)} addresses)\n")
                            for address in sorted(addresses):
                                f.write(f"{market}:{address}\n")
                            f.write("\n")

                logger.info(f"📝 Wrote {len(all_unique_addresses)} addresses to {lol_file}")
            except Exception as e:
                logger.error(f"Failed to write addresses to file: {e}")

            # Mark as successful
            metadata.status = ProcessingStatus.SUCCESS
            metadata.processed_at = datetime.now()
            self.processed_snapshots[metadata.hash] = metadata
            self._save_state()

            return result

        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing failed: {e}")
            metadata.status = ProcessingStatus.FAILED
        except Exception as e:
            logger.error(f"Error in extraction: {e}", exc_info=True)
            metadata.status = ProcessingStatus.FAILED

        return result

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

    async def _delayed_cleanup(self, path: Path, delay: int) -> None:
        """Delete file after delay."""
        await asyncio.sleep(delay)
        try:
            if path.exists():
                path.unlink()
        except Exception:
            passc