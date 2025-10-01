"""
Database manager for user metrics tables.
Handles all database operations with connection pooling and token-specific tables.
"""
import asyncpg
import logging
from pathlib import Path
from typing import Dict, List, Set, Optional, Any
from datetime import datetime
from contextlib import asynccontextmanager

from config.constants import DatabaseConfig
from .queries import UserMetricsQueries

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations for token-specific position monitoring."""

    def __init__(self, config):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None
        self.queries: Optional[UserMetricsQueries] = None

    async def initialize(self):
        """Initialize the database connection pool and schema."""

        try:
            # Create connection pool with statement_cache_size=0 for pgbouncer compatibility
            self.pool = await asyncpg.create_pool(
                self.config.database_url,
                min_size=DatabaseConfig.MIN_POOL_SIZE,
                max_size=DatabaseConfig.MAX_POOL_SIZE,
                command_timeout=DatabaseConfig.COMMAND_TIMEOUT,
                statement_cache_size=0,  # Disable statement cache for pgbouncer
                server_settings={
                    'jit': 'off'  # Disable JIT for better compatibility
                }
            )

            await self._create_schema()

            # Initialize query manager
            self.queries = UserMetricsQueries(self.pool)

            # Ensure tables exist for configured markets
            await self._ensure_market_tables()

            logger.info("Database initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    async def close(self):
        """Close database pool."""

        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")

    async def _create_schema(self):
        """
        Create database schema from external SQL file.
        This keeps the schema definition separate from application logic.
        """
        # Locate schema file relative to this module
        schema_file = Path(__file__).parent / "schema.sql"

        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        # Read schema SQL
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_sql = f.read()

        # Execute schema creation
        async with self.pool.acquire() as conn:
            await conn.execute(schema_sql)

            logger.info(f"Database schema initialized from {schema_file.name}")
            logger.info("Token-specific tables will be created as needed")

    async def _ensure_market_tables(self):
        """Dynamically ensure tables exist for all configured target markets."""
        # This method can be called periodically to check for new markets
        current_markets = set(self.config.target_markets)

        for market in current_markets:
            token = market.lower()
            if not await self.queries.verify_table_exists(token):
                await self.queries.create_token_table(token)
                logger.info(f"Created positions table for {market}")

        # Log current active markets
        logger.info(f"Active market tables: {', '.join(current_markets)}")

    async def reload_markets(self):
        """
        Reload TARGET_MARKETS from environment and ensure tables exist.
        This allows dynamic addition of markets without restart.
        """
        import os

        # Reload TARGET_MARKETS from environment
        markets_str = os.getenv("TARGET_MARKETS", "BTC,ETH,LINK")
        new_markets = [m.strip().upper() for m in markets_str.split(",") if m.strip()]

        # Check for new markets
        current_markets = set(self.config.target_markets)
        added_markets = set(new_markets) - current_markets

        if added_markets:
            logger.info(f"New markets detected: {', '.join(added_markets)}")
            self.config.target_markets = new_markets

            # Ensure tables exist for new markets
            await self._ensure_market_tables()

        return new_markets

    @asynccontextmanager
    async def transaction(self):
        """Get a database connection with transaction context."""

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    async def upsert_positions_batch(self, positions: List[Dict[str, Any]]):
        """
        Batch upsert positions to token-specific tables.

        Args:
            positions: List of position dictionaries
        """
        if not positions:
            return

        # Group positions by market/token
        positions_by_token = {}
        for pos in positions:
            token = pos['market'].lower()
            if token not in positions_by_token:
                positions_by_token[token] = []
            positions_by_token[token].append(pos)

        # Upsert positions for each token
        for token, token_positions in positions_by_token.items():
            await self.queries.upsert_positions(token, token_positions)

        logger.debug(f"Upserted {len(positions)} positions across {len(positions_by_token)} tokens")

    async def delete_positions_batch(self, positions: List[Dict[str, str]]):
        """
        Batch delete closed positions from token-specific tables.

        Args:
            positions: List of dicts with 'address' and 'market' keys
        """
        if not positions:
            return

        # Group positions by market/token
        positions_by_token = {}
        for pos in positions:
            token = pos['market'].lower()
            if token not in positions_by_token:
                positions_by_token[token] = []
            positions_by_token[token].append(pos)

        # Remove positions for each token
        for token, token_positions in positions_by_token.items():
            await self.queries.remove_positions(token, token_positions)

        logger.debug(f"Deleted {len(positions)} closed positions across {len(positions_by_token)} tokens")

    async def get_all_addresses(self) -> Dict[str, Set[str]]:
        """
        Get all addresses from all token tables grouped by market.

        Returns:
            Dictionary of market -> set of addresses
        """
        all_addresses = {}

        # Get addresses from each token table
        for market in self.config.target_markets:
            token = market.lower()
            token_addresses = await self.queries.get_active_addresses(
                token, self.config.min_position_size_usd
            )

            # Merge results
            for market_name, addresses in token_addresses.items():
                if market_name not in all_addresses:
                    all_addresses[market_name] = set()
                all_addresses[market_name].update(addresses)

        return all_addresses

    async def get_positions(
        self,
        market: Optional[str] = None,
        min_value: Optional[float] = None
    ) -> List[Dict]:
        """
        Get positions with optional filters from all token tables.

        Args:
            market: Optional market filter
            min_value: Optional minimum position value filter

        Returns:
            List of position dictionaries
        """
        all_positions = []

        # Get positions from each token table
        target_tokens = []
        if market:
            # If specific market requested, only query that token
            target_tokens = [market.lower()]
        else:
            # Query all configured markets
            target_tokens = [m.lower() for m in self.config.target_markets]

        for token in target_tokens:
            positions = await self.queries.get_filtered_positions(
                token=token,
                market=market,
                min_value=min_value,
                default_min_value=self.config.min_position_size_usd
            )
            all_positions.extend(positions)

        # Sort by position value descending
        all_positions.sort(key=lambda x: x.get('position_value', 0), reverse=True)
        return all_positions

    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics across all token tables."""

        overall_stats = {
            'unique_addresses': 0,
            'total_positions': 0,
            'total_value_usd': 0,
            'avg_position_value': 0,
            'max_position_value': 0,
            'oldest_update': None,
            'newest_update': None
        }

        all_market_stats = {}

        # Collect stats from each token table
        for market in self.config.target_markets:
            token = market.lower()

            # Get overall stats for this token
            token_overall = await self.queries.calculate_overall_stats(
                token, self.config.min_position_size_usd
            )

            # Get per-market stats for this token
            token_markets = await self.queries.calculate_market_stats(
                token, self.config.min_position_size_usd
            )

            # Aggregate overall stats
            if token_overall:
                overall_stats['unique_addresses'] += token_overall.get('unique_addresses', 0)
                overall_stats['total_positions'] += token_overall.get('total_positions', 0)
                overall_stats['total_value_usd'] += token_overall.get('total_value_usd', 0) or 0

                # Track max position value
                token_max = token_overall.get('max_position_value', 0) or 0
                if token_max > (overall_stats['max_position_value'] or 0):
                    overall_stats['max_position_value'] = token_max

                # Track oldest/newest updates
                token_oldest = token_overall.get('oldest_update')
                token_newest = token_overall.get('newest_update')

                if token_oldest and (not overall_stats['oldest_update'] or token_oldest < overall_stats['oldest_update']):
                    overall_stats['oldest_update'] = token_oldest

                if token_newest and (not overall_stats['newest_update'] or token_newest > overall_stats['newest_update']):
                    overall_stats['newest_update'] = token_newest

            # Merge market stats
            all_market_stats.update(token_markets)

        # Calculate average position value
        if overall_stats['total_positions'] > 0:
            overall_stats['avg_position_value'] = overall_stats['total_value_usd'] / overall_stats['total_positions']

        return {
            **overall_stats,
            'by_market': all_market_stats
        }

    async def cleanup_closed_positions(
        self,
        max_age_hours: int = DatabaseConfig.CLOSED_POSITION_MAX_AGE_HOURS
    ):
        """Clean up old closed positions from all token tables."""
        all_deleted = []

        # Cleanup from each token table
        for market in self.config.target_markets:
            token = market.lower()
            deleted = await self.queries.cleanup_closed_positions(token, max_age_hours)
            all_deleted.extend(deleted)

        if all_deleted:
            logger.info(f"Cleaned up {len(all_deleted)} confirmed closed positions "
                       f"(not updated in {max_age_hours} hours)")
            for row in all_deleted:
                logger.debug(f"Removed closed position: {row['address']}/{row['market']}")

        return all_deleted

    async def cleanup_stale_positions(
        self,
        max_age_hours: int = DatabaseConfig.STALE_POSITION_MAX_AGE_HOURS
    ):
        """Emergency cleanup of very old stale positions from all token tables."""
        all_deleted = []

        # Emergency cleanup from each token table
        for market in self.config.target_markets:
            token = market.lower()
            deleted = await self.queries.cleanup_stale_positions(token, max_age_hours)
            all_deleted.extend(deleted)

        if all_deleted:
            logger.warning(f"Emergency cleanup: Removed {len(all_deleted)} stale closed positions "
                          f"(not updated in {max_age_hours} hours)")
            for row in all_deleted:
                logger.debug(f"Emergency cleanup: {row['address']}/{row['market']}")

        return all_deleted