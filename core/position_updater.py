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
        self._sem = asyncio.Semaphore(getattr(self.config, 'http_concurrency', APIConfig.DEFAULT_HTTP_CONCURRENCY))
        self._retry_backoff = getattr(self.config, 'retry_backoff_sec', APIConfig.RETRY_BACKOFF_SEC)
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
        Update positions for all addresses - BATCH PROCESSING.

        Args:
            addresses_by_market: Dictionary of market -> addresses

        Returns:
            Dictionary of address -> position data
        """

        # Flatten addresses for batch processing
        all_addresses = []
        address_to_markets = {}  # Track which markets each address trades

        for market, addresses in addresses_by_market.items():
            for address in addresses:
                if address not in address_to_markets:
                    address_to_markets[address] = []
                address_to_markets[address].append(market)
                all_addresses.append(address)

        # Remove duplicates and sort for consistency
        unique_addresses = sorted(list(set(all_addresses)))
        logger.info(f"Processing {len(unique_addresses)} unique addresses in BATCHES...")

        # Debug: Show what addresses we have
        if not unique_addresses:
            logger.warning("No addresses to process! Check address_manager state:")
            logger.warning(f"addresses_by_market keys: {list(addresses_by_market.keys())}")
            for market, addrs in addresses_by_market.items():
                logger.warning(f"  {market}: {len(addrs)} addresses")
            return {}

        # BATCH PROCESSING - multiple addresses at once
        all_positions = {}
        total_api_failures = 0
        total_no_positions = 0
        total_positions_found = 0
        successful_addresses = 0

        # Process in batches
        batch_size = APIConfig.POSITION_BATCH_SIZE
        total_batches = (len(unique_addresses) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(unique_addresses))
            batch_addresses = unique_addresses[start_idx:end_idx]

            logger.info(f"🔄 Processing batch {batch_num + 1}/{total_batches} ({len(batch_addresses)} addresses)")

            try:
                # Process batch of addresses
                batch_results = await self._get_batch_positions(batch_addresses, address_to_markets)

                for address, positions in batch_results.items():
                    if positions is None:
                        # API failure - couldn't get response
                        total_api_failures += 1
                        logger.debug(f"❌ {address}: API failure")
                    elif len(positions) == 0:
                        # API succeeded but no positions in target markets
                        total_no_positions += 1
                        logger.debug(f"⚪ {address}: No positions in target markets")
                    else:
                        # Success - found positions
                        all_positions[address] = positions
                        positions_count = len(positions)
                        total_positions_found += positions_count
                        successful_addresses += 1
                        logger.debug(f"✓ {address}: {positions_count} positions")

                # Store all batch results at once
                if batch_results:
                    await self._store_positions(batch_results)

                # Progress update
                total_processed = successful_addresses + total_api_failures + total_no_positions
                success_rate = (successful_addresses / total_processed) * 100 if total_processed > 0 else 0
                logger.info(f"  Batch {batch_num + 1} complete: {successful_addresses} with positions, "
                           f"{total_no_positions} no positions, {total_api_failures} API failures ({success_rate:.1f}% found positions)")

                # Rate limiting between batches
                await asyncio.sleep(APIConfig.BATCH_DELAY)

            except Exception as e:
                logger.error(f"Failed to process batch {batch_num + 1}: {e}")
                total_api_failures += len(batch_addresses)
                # Longer delay after batch errors
                await asyncio.sleep(APIConfig.BATCH_ERROR_DELAY)
                continue

        # Final summary
        logger.info("=" * 60)
        logger.info("POSITION UPDATE SUMMARY")
        logger.info(f"Total addresses processed: {len(unique_addresses)}")
        logger.info(f"✓ Addresses with positions: {successful_addresses}")
        logger.info(f"⚪ Addresses with no positions: {total_no_positions}")
        logger.info(f"❌ API failures: {total_api_failures}")
        logger.info(f"📊 Total positions found: {total_positions_found}")
        success_rate = (successful_addresses / len(unique_addresses) * 100) if unique_addresses else 0.0
        avg_positions = (total_positions_found / successful_addresses) if successful_addresses > 0 else 0.0
        logger.info(f"Success rate: {success_rate:.1f}%")
        logger.info(f"Avg positions per address: {avg_positions:.1f}")
        logger.info("=" * 60)

        # Return positions dict as documented
        return all_positions

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
        positions: Dict[str, Dict[str, Dict]]
    ):
        """OPTIMIZED: Store positions using COPY for maximum efficiency."""
        if not positions:
            return

        try:
            # Get database pool from manager
            if not hasattr(self.db, 'pool') or self.db.pool is None:
                logger.error("Database pool is None - cannot store positions")
                logger.error(f"Database manager type: {type(self.db)}")
                logger.error(f"Database manager attributes: {dir(self.db)}")
                return

            async with self.db.pool.acquire() as conn:
                # Use a transaction for atomicity
                async with conn.transaction():
                    # Clear existing positions for these addresses
                    addresses = list(positions.keys())

                    # Delete in chunks to avoid query size limits
                    for i in range(0, len(addresses), 100):
                        chunk = addresses[i:i+100]
                        await conn.execute(
                            "DELETE FROM live_positions WHERE address = ANY($1)",
                            chunk
                        )

                    # Prepare batch insert data
                    records = []
                    for address, user_positions in positions.items():
                        if not isinstance(user_positions, dict):
                            logger.warning(f"Skipping invalid positions for {address}: {type(user_positions)}")
                            continue

                        for market, pos in user_positions.items():
                            # Skip closed positions
                            if not isinstance(pos, dict) or pos.get('closed'):
                                continue

                            # Get position size first to determine side
                            position_size = float(pos.get('position_size', 0))
                            if position_size == 0:
                                continue  # Skip zero positions

                            # Map position data to database fields - handle None values properly
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

                            records.append((
                                address.lower(),  # address VARCHAR(42)
                                market.upper(),   # market VARCHAR(20)
                                position_size,    # position_size NUMERIC(20, 8)
                                safe_float(pos.get('entry_price'), None),  # entry_price NUMERIC(20, 8)
                                safe_float(pos.get('liquidation_price'), None),  # liquidation_price NUMERIC(20, 8)
                                safe_float(pos.get('margin_used'), 0.0),  # margin_used NUMERIC(20, 8)
                                safe_float(pos.get('position_value'), 0.0),  # position_value NUMERIC(20, 8)
                                safe_float(pos.get('unrealized_pnl'), 0.0),  # unrealized_pnl NUMERIC(20, 8)
                                safe_float(pos.get('return_on_equity'), None),  # return_on_equity NUMERIC(10, 6)
                                pos.get('leverage', {}).get('type', 'cross'),  # leverage_type VARCHAR(10)
                                safe_int(pos.get('leverage', {}).get('value'), None),  # leverage_value INTEGER
                                safe_float(pos.get('leverage', {}).get('rawUsd'), 0.0),  # leverage_raw_usd NUMERIC(20, 8)
                                safe_float(pos.get('account_value'), 0.0),  # account_value NUMERIC(20, 8)
                                safe_float(pos.get('total_margin_used'), 0.0),  # total_margin_used NUMERIC(20, 8)
                                safe_float(pos.get('withdrawable'), 0.0),  # withdrawable NUMERIC(20, 8)
                                datetime.now()  # last_updated TIMESTAMP
                            ))

                    # Use COPY for fastest insertion
                    if records:
                        # Insert in smaller chunks
                        chunk_size = 500
                        for i in range(0, len(records), chunk_size):
                            chunk = records[i:i+chunk_size]
                            await conn.executemany(
                                """
                                INSERT INTO live_positions (
                                    address, market, position_size, entry_price, liquidation_price,
                                    margin_used, position_value, unrealized_pnl, return_on_equity,
                                    leverage_type, leverage_value, leverage_raw_usd, account_value,
                                    total_margin_used, withdrawable, last_updated
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                                ON CONFLICT (address, market) DO UPDATE SET
                                    position_size = EXCLUDED.position_size,
                                    entry_price = EXCLUDED.entry_price,
                                    liquidation_price = EXCLUDED.liquidation_price,
                                    margin_used = EXCLUDED.margin_used,
                                    position_value = EXCLUDED.position_value,
                                    unrealized_pnl = EXCLUDED.unrealized_pnl,
                                    return_on_equity = EXCLUDED.return_on_equity,
                                    leverage_type = EXCLUDED.leverage_type,
                                    leverage_value = EXCLUDED.leverage_value,
                                    leverage_raw_usd = EXCLUDED.leverage_raw_usd,
                                    account_value = EXCLUDED.account_value,
                                    total_margin_used = EXCLUDED.total_margin_used,
                                    withdrawable = EXCLUDED.withdrawable,
                                    last_updated = EXCLUDED.last_updated
                                """,
                                chunk
                            )

                        logger.info(f"✓ Stored {len(records)} positions for {len(addresses)} addresses")

        except Exception as e:
            logger.error(f"Database write failed: {e}")
            # Continue anyway - don't crash the whole process

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
