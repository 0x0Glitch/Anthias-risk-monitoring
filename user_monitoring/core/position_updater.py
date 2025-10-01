"""
Position updater using clearinghouseState API.
Handles batch queries and updates live_positions table.
"""
import asyncio
import aiohttp
import logging
from typing import Dict, List, Optional, Set, Any
from datetime import datetime

from config.constants import APISource, APIConfig
from core.utils import safe_float

logger = logging.getLogger(__name__)


class PositionUpdater:
    """Updates user positions via clearinghouseState API."""

    def __init__(self, config, db_manager):
        self.config = config
        self.db = db_manager
        self.session: Optional[aiohttp.ClientSession] = None

        # Limit concurrent HTTP calls to avoid rate limits
        self._sem = asyncio.Semaphore(getattr(self.config, 'max_workers', APIConfig.DEFAULT_HTTP_CONCURRENCY))
        self._retry_backoff = getattr(self.config, 'retry_delay', APIConfig.RETRY_BACKOFF_SEC)
        self._api_call_delay = APIConfig.API_CALL_DELAY

        # Track API performance
        self.api_stats = {
            'nvn_success': 0,
            'nvn_failures': 0,
            'public_success': 0,
            'public_failures': 0,
            'total_queries': 0
        }

    async def start(self):
        """Initialize the position updater."""
        timeout = aiohttp.ClientTimeout(total=self.config.api_timeout)
        self.session = aiohttp.ClientSession(timeout=timeout)
        logger.info("Position updater started")

    async def stop(self):
        """Clean up resources."""
        if self.session:
            await self.session.close()
        logger.info(f"Position updater stopped. Stats: {self.api_stats}")

    async def update_positions(self, addresses_by_market: Dict[str, Set[str]]) -> Dict[str, Dict]:
        """
        Update positions for all addresses - MARKET-BY-MARKET PROCESSING.
        Each market is processed separately with its own batches and logging.

        Args:
            addresses_by_market: Dictionary of market -> addresses

        Returns:
            Dictionary of address -> position data
        """

        all_positions = {}

        if not addresses_by_market:
            logger.info("No addresses to update")
            return {}

        # Calculate totals for overall summary
        total_unique_addresses = set()
        for addresses in addresses_by_market.values():
            total_unique_addresses.update(addresses)

        total_addresses = len(total_unique_addresses)
        total_markets = len(addresses_by_market)

        # Overall summary
        logger.info("=" * 80)
        logger.info("STARTING MULTI-MARKET POSITION UPDATE")
        logger.info(f"Markets to process: {', '.join(addresses_by_market.keys())}")
        logger.info(f"Total unique addresses: {total_addresses}")
        logger.info(f"Markets: {total_markets}")
        logger.info("=" * 80)

        # Process each market separately
        overall_stats = {
            'successful_addresses': 0,
            'total_api_failures': 0,
            'total_no_positions': 0,
            'total_positions_found': 0
        }

        for market_num, (market, addresses) in enumerate(addresses_by_market.items(), 1):
            if not addresses:
                logger.info(f"📍 Market {market_num}/{total_markets} - {market}: No addresses to process")
                continue

            logger.info("=" * 60)
            logger.info(f"📍 PROCESSING MARKET {market_num}/{total_markets}: {market}")
            logger.info(f"Addresses in {market}: {len(addresses)}")
            logger.info("=" * 60)

            # Process this market completely before moving to next
            market_positions, market_stats = await self._process_market_addresses(market, list(addresses))

            # Wait for all database operations to complete before counting
            await asyncio.sleep(0.5)

            # ENHANCED VERIFICATION: Check database count after processing
            actual_db_count = await self.db.queries.get_positions_count(market.lower())
            expected_count = len(market_positions)

            if actual_db_count != expected_count:
                logger.error(f"❌ {market} COUNT MISMATCH:")
                logger.error(f"   Expected: {expected_count} positions")
                logger.error(f"   Database: {actual_db_count} positions")
                logger.error(f"   Difference: {actual_db_count - expected_count:+d}")

                # DEBUG: Get sample of addresses in DB for debugging
                db_addresses = await self.db.queries.get_all_addresses_in_market(market.lower())
                processed_addresses = set(market_positions.keys())

                # Find addresses in DB but not in processed set
                extra_in_db = [addr for addr in db_addresses if addr not in processed_addresses]
                missing_from_db = [addr for addr in processed_addresses if addr not in db_addresses]

                if extra_in_db:
                    logger.error(f"   🔍 Extra addresses in DB: {extra_in_db[:3]}... ({len(extra_in_db)} total)")
                    # IMMEDIATE FIX: Remove extra addresses
                    logger.warning(f"   🗑️ Removing {len(extra_in_db)} extra addresses from database...")
                    await self.db.queries.bulk_remove_addresses(market.lower(), extra_in_db)
                    logger.info(f"   ✅ Removed {len(extra_in_db)} extra addresses")

                if missing_from_db:
                    logger.error(f"   🔍 Missing from DB: {missing_from_db[:3]}... ({len(missing_from_db)} total)")
                    # CRITICAL FIX: Re-process missing addresses to add them back
                    logger.warning(f"   ➕ Re-processing {len(missing_from_db)} missing addresses...")

                    # Get positions for missing addresses
                    missing_positions = {}
                    for addr in missing_from_db:
                        if addr in market_positions:
                            missing_positions[addr] = {market: market_positions[addr]}

                    if missing_positions:
                        # Force upsert the missing positions
                        missing_records = []
                        for addr, pos_data in missing_positions.items():
                            pos = pos_data[market]
                            missing_records.append({
                                'address': addr.lower(),
                                'market': market.upper(),
                                'position_size': float(pos.get('position_size', 0)),
                                'entry_price': float(pos.get('entry_price', 0)),
                                'liquidation_price': float(pos.get('liquidation_price', 0)),
                                'margin_used': float(pos.get('margin_used', 0)),
                                'position_value': float(pos.get('position_value', 0)),
                                'unrealized_pnl': float(pos.get('unrealized_pnl', 0)),
                                'return_on_equity': float(pos.get('return_on_equity', 0)),
                                'leverage_type': pos.get('leverage', {}).get('type', 'cross'),
                                'leverage_value': int(pos.get('leverage', {}).get('value', 0)) if pos.get('leverage', {}).get('value') else None,
                                'leverage_raw_usd': float(pos.get('leverage', {}).get('rawUsd', 0)),
                                'account_value': float(pos.get('account_value', 0)),
                                'total_margin_used': float(pos.get('total_margin_used', 0)),
                                'withdrawable': float(pos.get('withdrawable', 0))
                            })

                        await self.db.queries.upsert_positions(market.lower(), missing_records)
                        logger.info(f"   ✅ Added {len(missing_records)} missing positions to database")

                        # Verify the fix worked
                        await asyncio.sleep(0.2)  # Allow DB to commit
                        final_db_count = await self.db.queries.get_positions_count(market.lower())
                        if final_db_count == expected_count:
                            logger.info(f"   🎯 FIXED: Database now has {final_db_count} positions (perfect match!)")
                        else:
                            logger.error(f"   ❌ STILL BROKEN: Database has {final_db_count}, expected {expected_count}")
            else:
                logger.info(f"✅ {market} COUNT VERIFIED: {actual_db_count} positions match expected")

            # Merge results
            all_positions.update(market_positions)

            # Update overall stats
            overall_stats['successful_addresses'] += market_stats['successful']
            overall_stats['total_api_failures'] += market_stats['failures']
            overall_stats['total_no_positions'] += market_stats['no_positions']
            overall_stats['total_positions_found'] += market_stats['positions_found']

            logger.info(f"✅ {market} complete: {market_stats['successful']} addresses with {expected_count} positions")
            logger.info(f"📊 {market} database verified: {actual_db_count} positions")

            # Longer pause between markets to ensure complete isolation
            if market_num < total_markets:
                logger.info("⏳ Waiting 2 seconds before processing next market...")
                await asyncio.sleep(2)

        # Final overall summary
        logger.info("=" * 80)
        logger.info("MULTI-MARKET UPDATE SUMMARY")
        logger.info(f"Markets processed: {total_markets}")
        logger.info(f"✓ Addresses with positions: {overall_stats['successful_addresses']}")
        logger.info(f"⚪ Addresses with no positions: {overall_stats['total_no_positions']}")
        logger.info(f"❌ API failures: {overall_stats['total_api_failures']}")
        logger.info(f"📊 Total positions found: {overall_stats['total_positions_found']}")

        return all_positions

    async def _process_market_addresses(self, market: str, addresses: List[str]) -> tuple[Dict[str, Dict], Dict[str, int]]:
        """
        Process addresses for a specific market with batch processing.

        Args:
            market: Market name (e.g., 'BTC', 'ETH', 'LINK')
            addresses: List of addresses to process for this market

        Returns:
            Tuple of (market_positions, market_stats)
        """

        market_positions = {}
        batch_size = self.config.position_refresh_batch_size
        total_batches = (len(addresses) + batch_size - 1) // batch_size

        # Counters for this market
        successful_addresses = 0
        api_failures = 0
        no_positions = 0
        positions_found = 0

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(addresses))
            batch_addresses = addresses[start_idx:end_idx]

            logger.info(f"  🔄 {market} batch {batch_num + 1}/{total_batches} ({len(batch_addresses)} addresses)")
            logger.debug(f"    📋 Batch {batch_num + 1} addresses: {start_idx}-{end_idx-1}")

            try:
                # Create address to markets mapping for this specific market
                address_to_markets = {addr: [market] for addr in batch_addresses}

                # Process batch of addresses
                batch_results = await self._get_batch_positions(batch_addresses, address_to_markets)

                for address, positions in batch_results.items():
                    if positions is None:
                        api_failures += 1
                        logger.debug(f"    ❌ {address}: API failure")
                    elif len(positions) == 0:
                        no_positions += 1
                        logger.debug(f"    ⚪ {address}: No positions in {market}")
                    else:
                        market_positions[address] = positions
                        positions_count = len(positions)
                        positions_found += positions_count
                        successful_addresses += 1
                        logger.debug(f"    ✓ {address}: {positions_count} positions")

                # Store batch results - pass ALL batch addresses for proper cleanup
                logger.debug(f"    💾 Storing {len(batch_results)} address results to database...")
                await self._store_positions(batch_results, market, batch_addresses)

                # Ensure database write is fully committed before continuing
                await asyncio.sleep(0.2)  # Longer delay to ensure write completion
                logger.debug(f"    ✅ Database write completed")

                # Progress update for this market
                total_processed = successful_addresses + api_failures + no_positions
                success_rate = (successful_addresses / total_processed) * 100 if total_processed > 0 else 0
                logger.info(f"    Batch {batch_num + 1} complete: {successful_addresses} with positions, "
                           f"{no_positions} no positions, {api_failures} API failures ({success_rate:.1f}% found positions)")

                # Rate limiting between batches
                await asyncio.sleep(APIConfig.BATCH_DELAY)

            except Exception as e:
                logger.error(f"    ❌ FAILED to process {market} batch {batch_num + 1}/{total_batches}: {e}")
                logger.error(f"    ⏭️  Skipping {len(batch_addresses)} addresses and continuing to next batch...")
                api_failures += len(batch_addresses)
                await asyncio.sleep(APIConfig.BATCH_ERROR_DELAY)
                continue

        market_stats = {
            'successful': successful_addresses,
            'failures': api_failures,
            'no_positions': no_positions,
            'positions_found': positions_found
        }

        return market_positions, market_stats

    async def _verify_market_positions_count(self, market: str) -> int:
        """Verify the actual count of positions in the database for a specific market."""
        try:
            # Query database to get actual count
            token = market.lower()
            count = await self.db.queries.get_positions_count(token)
            return count
        except Exception as e:
            logger.error(f"Failed to verify positions count for {market}: {e}")
            return -1

    async def _get_batch_positions(
        self,
        batch_addresses: List[str],
        address_to_markets: Dict[str, List[str]]
    ) -> Dict[str, Dict[str, Any]]:
        """Get positions for a batch of addresses via concurrent API calls."""

        batch_results = {}

        # Create concurrent tasks for all addresses in the batch
        tasks = []
        for address in batch_addresses:
            markets = address_to_markets.get(address, list(self.config.target_markets))
            task = asyncio.create_task(
                self._get_user_positions_safe(address, markets),
                name=f"pos_{address[:8]}"
            )
            tasks.append((address, task))

        # Wait for all tasks to complete with timeout
        timeout = APIConfig.BATCH_TIMEOUT
        try:
            await asyncio.wait_for(
                asyncio.gather(*[task for _, task in tasks], return_exceptions=True),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning(f"Batch timeout after {timeout}s, collecting partial results")

        # Collect results from completed tasks
        for address, task in tasks:
            try:
                if task.done():
                    result = task.result()
                    if isinstance(result, Exception):
                        logger.error(f"Task error for {address}: {result}")
                        batch_results[address] = None
                    else:
                        batch_results[address] = result
                else:
                    logger.warning(f"Task not completed for {address}")
                    task.cancel()
                    batch_results[address] = None
            except Exception as e:
                logger.error(f"Error collecting result for {address}: {e}")
                batch_results[address] = None

        return batch_results

    async def _get_user_positions_safe(
        self,
        address: str,
        target_markets: List[str]
    ) -> Optional[Dict[str, Any]]:
        """Safe wrapper for _get_user_positions with error handling."""
        try:
            return await self._get_user_positions(address, target_markets)
        except Exception as e:
            logger.error(f"Error getting positions for {address}: {e}")
            return None

    async def _get_user_positions(
        self,
        address: str,
        target_markets: List[str]
    ) -> Optional[Dict[str, Any]]:
        """Get positions for a single user via API."""

        try:
            # Try NVN first, fallback to public
            state = await self._query_clearinghouse_state(address, APISource.NVN)
            if not state:
                state = await self._query_clearinghouse_state(address, APISource.PUBLIC)

            if not state:
                logger.debug(f"No clearinghouse state for {address}")
                return None
        except Exception as e:
            logger.error(f"Failed to get positions for {address}: {e}")
            return None

        # Extract positions for target markets
        positions = {}
        filtered_count = 0
        processed_count = 0

        asset_positions = state.get('assetPositions', [])
        margin_summary = state.get('marginSummary', {})

        for asset_pos in asset_positions:
            position = asset_pos.get('position', {})
            coin = position.get('coin', '').upper()

            processed_count += 1

            # Skip if not in target markets
            if coin not in target_markets:
                continue

            # Extract size - this is a STRING in the API response
            szi_str = position.get('szi', '0')
            szi = safe_float(szi_str, 0.0)

            # Skip if no position
            if abs(szi) == 0:
                continue

            # Extract ALL fields as strings first (API returns strings)
            entry_px = safe_float(position.get('entryPx'))
            position_value_usd = safe_float(position.get('positionValue'))
            unrealized_pnl = safe_float(position.get('unrealizedPnl'))
            return_on_equity = safe_float(position.get('returnOnEquity'))
            liquidation_px = safe_float(position.get('liquidationPx'))
            margin_used = safe_float(position.get('marginUsed'))

            # Check minimum threshold - but be more lenient to avoid losing positions
            if position_value_usd and position_value_usd < self.config.min_position_size_usd:
                filtered_count += 1
                logger.debug(f"Filtering out {coin} position: ${position_value_usd:.2f} < ${self.config.min_position_size_usd}")
                continue

            # Get leverage info - it's a nested dict
            leverage_info = position.get('leverage', {})
            leverage_type = (leverage_info.get('type') or 'cross').lower()
            leverage_value = safe_float(leverage_info.get('value'))
            leverage_raw_usd = safe_float(leverage_info.get('rawUsd'))

            # Use liquidation price directly from API (no manual calculation)
            final_liquidation_px = liquidation_px

            # Store the position with all data
            positions[coin] = {
                'position_size': szi,
                'entry_price': entry_px,
                'liquidation_price': final_liquidation_px,  # Direct from API
                'margin_used': margin_used,
                'position_value': position_value_usd or abs(szi * entry_px) if entry_px else 0,
                'unrealized_pnl': unrealized_pnl,
                'return_on_equity': return_on_equity,
                'leverage': {
                    'type': leverage_type,
                    'value': leverage_value,
                    'rawUsd': leverage_raw_usd
                },
                'account_value': safe_float(margin_summary.get('accountValue')),
                'total_margin_used': safe_float(margin_summary.get('totalMarginUsed')),
                'withdrawable': safe_float(state.get('withdrawable'))  # Top-level field
            }

        # Log processing stats to identify the gap
        if processed_count > 0:
            logger.debug(f"Address {address}: processed {processed_count} positions, filtered {filtered_count}, stored {len(positions)}")

            # If we filtered out many positions, log a warning
            if filtered_count > 0 and filtered_count > len(positions):
                logger.warning(f"Address {address}: filtered out {filtered_count} positions, only kept {len(positions)}")

        return positions

    # REMOVED: _calculate_liquidation_price method - using API values only

    def _get_maintenance_leverage(self, position_value: float, coin: str) -> float:
        """
        Get maintenance leverage based on position value and margin tiers.

        Based on Hyperliquid's actual margin tier system.
        For assets with margin tiers, maintenance leverage depends on the
        position value at the liquidation price.
        """

        # Hyperliquid-style margin tiers (maintenance leverage values)
        # These are approximations based on typical DeFi perp exchange values
        tiers = {}

        # Get tiers for this coin, default to DEFAULT
        coin_tiers = tiers.get(coin, tiers['DEFAULT'])

        # Find appropriate tier (use reversed to find highest matching tier)
        for threshold, maintenance_leverage in reversed(coin_tiers):
            if position_value >= threshold:
                return maintenance_leverage

        # Fallback to most conservative tier
        return coin_tiers[0][1]

    def _get_maintenance_margin_rate(self, position_value: float, coin: str) -> float:
        """
        Get maintenance margin rate based on position value and asset.

        Maintenance margin rate determines how close to liquidation a position can get.
        Typical values:
        - Major assets (BTC, ETH): 3-5%
        - Altcoins: 5-10%
        - Small positions: Lower rates
        - Large positions: Higher rates (risk tiers)
        """

        # Risk tiers based on position size
        if position_value < 10_000:  # Small positions
            base_rate = 0.03  # 3%
        elif position_value < 50_000:  # Medium positions
            base_rate = 0.05  # 5%
        elif position_value < 200_000:  # Large positions
            base_rate = 0.08  # 8%
        else:  # Very large positions
            base_rate = 0.12  # 12%

        # Asset-specific adjustments
        if coin in ['BTC', 'ETH']:
            # Major assets get lower maintenance margin
            return base_rate * 0.8
        elif coin in ['LINK', 'SOL', 'AVAX', 'MATIC']:
            # Popular altcoins
            return base_rate
        else:
            # Other assets get higher maintenance margin
            return base_rate * 1.2

    async def _fetch_mark_prices(self) -> Dict[str, float]:
        """Fetch current mark prices from local node or public API."""
        try:
            # Try local node info server first (if available)
            if hasattr(self.config, 'local_node_url'):
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                    payload = {"type": "activeAssetData"}
                    async with session.post(f"{self.config.local_node_url}/info", json=payload) as response:
                        if response.status == 200:
                            data = await response.json()
                            mark_prices = {}
                            for asset_data in data:
                                coin = asset_data.get('coin', '').upper()
                                mark_px = float(asset_data.get('markPx', 0))
                                if coin and mark_px > 0:
                                    mark_prices[coin] = mark_px
                            return mark_prices
        except Exception as e:
            logger.debug(f"Failed to fetch mark prices from local node: {e}")

        # Fallback to public API or return empty dict
        return {}

    async def _fetch_margin_table(self) -> Dict[str, List]:
        """Fetch margin tier table from local node."""
        try:
            if hasattr(self.config, 'local_node_url'):
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                    payload = {"type": "marginTable"}
                    async with session.post(f"{self.config.local_node_url}/info", json=payload) as response:
                        if response.status == 200:
                            return await response.json()
        except Exception as e:
            logger.debug(f"Failed to fetch margin table from local node: {e}")

        return {}

    async def _query_clearinghouse_state(
        self,
        address: str,
        source: APISource
    ) -> Optional[Dict]:
        """Query clearinghouseState from specified API source."""

        # Try local node first if configured
        if hasattr(self.config, 'local_node_url') and source == APISource.NVN:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                    payload = {"type": "clearinghouseState", "user": address}
                    async with session.post(f"{self.config.local_node_url}/info", json=payload) as response:
                        if response.status == 200:
                            return await response.json()
            except Exception as e:
                logger.debug(f"Local node query failed for {address}: {e}")

        url = self.config.nvn_api_url if source == APISource.NVN else self.config.public_api_url

        # Ensure address is properly formatted
        normalized_address = address.lower().strip()
        if not normalized_address.startswith('0x'):
            normalized_address = f"0x{normalized_address}"

        payload = {
            "type": "clearinghouseState",
            "user": normalized_address
        }

        for attempt in range(self.config.max_retries):
            try:
                async with self.session.post(url, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()

                        if source == APISource.NVN:
                            self.api_stats['nvn_success'] += 1
                        else:
                            self.api_stats['public_success'] += 1

                        return data
                    else:
                        logger.warning(f"{source.value} API status {response.status} for {address}")
                        # Backoff especially for 429 (rate limit) or 5xx errors
                        if response.status in [429, 500, 502, 503, 504]:
                            await asyncio.sleep(self._retry_backoff * (attempt + 1))

            except asyncio.TimeoutError:
                logger.warning(f"{source.value} API timeout for {address} (attempt {attempt + 1})")
            except Exception as e:
                logger.error(f"{source.value} API error for {address}: {e}")

            if attempt < self.config.max_retries - 1:
                await asyncio.sleep(self.config.retry_delay)

        if source == APISource.NVN:
            self.api_stats['nvn_failures'] += 1
        else:
            self.api_stats['public_failures'] += 1

        logger.debug(f"All API attempts failed for {address}")
        return None

    async def _store_positions(
        self,
        positions: Dict[str, Dict[str, Dict]],
        market: str,
        all_batch_addresses: List[str]
    ):
        """
        CRITICAL FIX: Properly handle both active and closed positions.

        - Addresses WITH positions → UPSERT
        - Addresses WITH NO/CLOSED positions → DELETE from DB
        - API failures → Skip (don't touch DB)

        Args:
            positions: Dict of address -> positions data (None = API failure, {} = no positions)
            market: The market being processed
            all_batch_addresses: ALL addresses in this batch
        """
        if not all_batch_addresses:
            return

        try:
            # Get database pool from manager
            if not hasattr(self.db, 'pool') or self.db.pool is None:
                logger.error("Database pool is None - cannot store positions")
                return

            # CRITICAL: Separate addresses into 3 categories
            addresses_with_positions = []
            addresses_to_remove = []  # No positions or closed positions
            addresses_to_skip = []     # API failures

            for address in all_batch_addresses:
                user_positions = positions.get(address)

                # Check if API call failed
                if user_positions is None:
                    addresses_to_skip.append(address)
                    continue

                # Check if address has any active position in this market
                has_active_position = False

                if isinstance(user_positions, dict):
                    for pos_market, pos in user_positions.items():
                        if pos_market != market:
                            continue

                        # CRITICAL: More strict filtering
                        if isinstance(pos, dict) and not pos.get('closed'):
                            position_size = float(pos.get('position_size', 0))
                            entry_price = float(pos.get('entry_price', 0))
                            position_value_usd = float(pos.get('position_value', 0))

                            # STRICT: Must have non-zero position, valid entry price, AND minimum USD value
                            if (position_size != 0 and
                                entry_price > 0 and
                                position_value_usd >= self.config.min_position_value_usd):
                                has_active_position = True
                                addresses_with_positions.append(address)
                                break

                # If no active position found, mark for removal
                if not has_active_position and user_positions is not None:
                    addresses_to_remove.append(address)

            # Build position records ONLY for addresses WITH positions
            market_records = []
            seen_addresses = set()  # Prevent duplicates

            for address in addresses_with_positions:
                # CRITICAL: Skip if we already processed this address
                if address in seen_addresses:
                    logger.warning(f"⚠️ Duplicate address detected: {address}")
                    continue

                user_positions = positions.get(address)
                if not user_positions or not isinstance(user_positions, dict):
                    continue

                # Get positions for this specific market
                for pos_market, pos in user_positions.items():
                    if pos_market != market:
                        continue

                    # STRICT: Skip closed, invalid, or zero-size positions
                    if not isinstance(pos, dict) or pos.get('closed'):
                        continue

                    position_size = float(pos.get('position_size', 0))
                    entry_price = float(pos.get('entry_price', 0))
                    position_value_usd = float(pos.get('position_value', 0))

                    # CRITICAL: Must have non-zero position, valid entry price, AND minimum USD value
                    if position_size == 0 or entry_price <= 0 or position_value_usd < self.config.min_position_value_usd:
                        logger.debug(f"   Skipping {address}: size={position_size}, price={entry_price}, value_usd=${position_value_usd:.2f}")
                        continue

                    # Add to records and mark as seen
                    market_records.append({
                        'address': address,
                        'position': pos
                    })
                    seen_addresses.add(address)
                    break  # Only one position per address per market

            # Helper functions for safe type conversion
            def safe_float(value, default=0.0):
                """Safely convert to float, handling None values."""
                if value is None:
                    return default if default is not None else None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default if default is not None else None

            def safe_int(value, default=None):
                """Safely convert to int, handling None values."""
                if value is None:
                    return default
                try:
                    return int(float(value))  # Convert via float first for strings like "10.0"
                except (ValueError, TypeError):
                    return default

            # Prepare position records with proper data types
            position_records = []
            for item in market_records:
                address = item['address']
                pos = item['position']

                position_size = float(pos.get('position_size', 0))

                position_records.append({
                    'address': address.lower(),
                    'market': market.upper(),
                    'position_size': position_size,
                    'entry_price': safe_float(pos.get('entry_price'), None),
                    'liquidation_price': safe_float(pos.get('liquidation_price'), None),
                    'margin_used': safe_float(pos.get('margin_used'), 0.0),
                    'position_value': safe_float(pos.get('position_value'), 0.0),
                    'unrealized_pnl': safe_float(pos.get('unrealized_pnl'), 0.0),
                    'return_on_equity': safe_float(pos.get('return_on_equity'), None),
                    'leverage_type': pos.get('leverage', {}).get('type', 'cross'),
                    'leverage_value': safe_int(pos.get('leverage', {}).get('value'), None),
                    'leverage_raw_usd': safe_float(pos.get('leverage', {}).get('rawUsd'), 0.0),
                    'account_value': safe_float(pos.get('account_value'), 0.0),
                    'total_margin_used': safe_float(pos.get('total_margin_used'), 0.0),
                    'withdrawable': safe_float(pos.get('withdrawable'), 0.0)
                })

            # CRITICAL: Handle all three cases properly
            # 1. UPSERT positions for addresses WITH positions
            if position_records:
                await self._upsert_positions(market, position_records)
                logger.debug(f"✓ {market}: Upserted {len(position_records)} positions")

            # 2. DELETE positions for addresses with NO/CLOSED positions
            if addresses_to_remove:
                token = market.lower()
                await self.db.queries.bulk_remove_addresses(token, addresses_to_remove)
                logger.debug(f"🗑️ {market}: Removed {len(addresses_to_remove)} addresses with closed/no positions")

            # 3. Skip API failures (don't touch DB)
            if addresses_to_skip:
                logger.debug(f"⚠️ {market}: Skipped {len(addresses_to_skip)} addresses due to API failures")

            # DETAILED LOGGING for debugging mismatches
            logger.info(f"📊 {market} Batch Details:")
            logger.info(f"   📥 Input: {len(all_batch_addresses)} addresses")
            logger.info(f"   ✅ Active positions: {len(addresses_with_positions)} addresses → {len(position_records)} records")
            logger.info(f"   🗑️ To remove: {len(addresses_to_remove)} addresses")
            logger.info(f"   ⚠️ API failures: {len(addresses_to_skip)} addresses")

            # Log any discrepancy between active addresses and position records
            if len(addresses_with_positions) != len(position_records):
                logger.warning(f"⚠️ {market} DISCREPANCY: {len(addresses_with_positions)} active addresses "
                              f"but only {len(position_records)} position records created!")
                logger.warning("   This suggests some addresses have invalid position data")

        except Exception as e:
            logger.error(f"Database write failed: {e}")

    async def _upsert_positions(self, market: str, positions: List[Dict]):
        """
        UPSERT positions for addresses. Frontend-friendly - no unnecessary deletes.
        Uses INSERT ... ON CONFLICT DO UPDATE for atomic upserts.
        """
        if not positions:
            return

        token = market.lower()
        try:
            await self.db.queries.upsert_positions(token, positions)
        except Exception as e:
            logger.error(f"❌ {market}: Failed to upsert positions: {e}")
            raise

    async def cleanup_against_snapshot(self, snapshot_addresses_by_market: Dict[str, Set[str]]):
        try:
            total_removed = 0
            logger.info("=" * 80)
            logger.info("🧹 STARTING AGGRESSIVE DATABASE CLEANUP")
            logger.info("=" * 80)

            for market, snapshot_addresses in snapshot_addresses_by_market.items():
                token = market.lower()

                # Get all addresses currently in database for this market
                db_addresses = await self.db.queries.get_all_addresses_in_market(token)
                db_set = set(addr.lower() for addr in db_addresses)

                # Snapshot addresses are the source of truth
                snapshot_set = set(addr.lower() for addr in snapshot_addresses)

                # Find addresses in DB but NOT in snapshot
                addresses_to_remove = list(db_set - snapshot_set)

                if addresses_to_remove:
                    logger.warning(f"⚠️ {market}: Found {len(addresses_to_remove)} STALE addresses in database")
                    logger.info(f"   DB has: {len(db_addresses)} addresses")
                    logger.info(f"   Snapshot has: {len(snapshot_addresses)} addresses")
                    logger.info(f"   Removing: {len(addresses_to_remove)} stale addresses")

                    # Remove in chunks to avoid query size limits
                    chunk_size = 100
                    for i in range(0, len(addresses_to_remove), chunk_size):
                        chunk = addresses_to_remove[i:i+chunk_size]
                        await self.db.queries.bulk_remove_addresses(token, chunk)

                    logger.info(f"✅ {market}: Removed {len(addresses_to_remove)} stale addresses from database")
                    total_removed += len(addresses_to_remove)
                else:
                    logger.info(f"✓ {market}: Database clean - all {len(db_addresses)} addresses match snapshot")

            logger.info("=" * 80)
            if total_removed > 0:
                logger.warning(f"🎯 CLEANUP COMPLETE: Removed {total_removed} stale addresses across all markets")
            else:
                logger.info(f"✅ DATABASE CLEAN: All addresses match snapshot perfectly")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Failed to cleanup against snapshot: {e}")
            raise

    async def _clear_closed_positions(
        self,
        market: str,
        addresses: List[str]
    ):
        """Clear positions for addresses that no longer have positions in this market."""
        if not addresses:
            return

        token = market.lower()
        try:
            await self.db.queries.bulk_remove_addresses(token, addresses)
            logger.debug(f"✓ {market}: Cleared {len(addresses)} addresses with closed positions")
        except Exception as e:
            logger.error(f"❌ {market}: Failed to clear closed positions: {e}")

    async def check_removal_candidates(
        self,
        removal_candidates: Dict[str, Set[str]]
    ) -> Dict[str, Set[str]]:
        """
        Check removal candidates for closed positions (dual-check).

        Args:
            removal_candidates: Dictionary of market -> addresses to check

        Returns:
            Dictionary of market -> addresses with confirmed closed positions
        """

        closed_positions = {market: set() for market in self.config.target_markets}

        for market, addresses in removal_candidates.items():
            if not addresses:
                continue

            logger.info(f"Checking {len(addresses)} {market} removal candidates")

            for address in addresses:
                positions = await self._get_user_positions(address, [market])

                # Check if position is closed or doesn't exist
                if not positions or not positions.get(market) or positions.get(market, {}).get('closed', False):
                    closed_positions[market].add(address)

        return closed_positions

    def get_stats(self) -> Dict[str, int]:
        """Get API query statistics."""
        return self.api_stats.copy()