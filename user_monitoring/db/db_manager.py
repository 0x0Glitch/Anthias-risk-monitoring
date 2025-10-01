"""
Database manager for live_positions table.
Handles all database operations with connection pooling.
"""
import asyncpg
import logging
from pathlib import Path
from typing import Dict, List, Set, Optional, Any
from datetime import datetime
from contextlib import asynccontextmanager

from config.constants import DatabaseConfig

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations for position monitoring."""
    
    def __init__(self, config):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None
    
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
            
            # Verify table structure
            result = await conn.fetchrow("""
                SELECT COUNT(*) as col_count 
                FROM information_schema.columns 
                WHERE table_name = 'live_positions'
            """)
            
            logger.info(f"Database schema initialized from {schema_file.name}")
            logger.info(f"live_positions table has {result['col_count']} columns")
    
    @asynccontextmanager
    async def transaction(self):
        """Get a database connection with transaction context."""
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                yield conn
    
    async def upsert_positions_batch(self, positions: List[Dict[str, Any]]):
        """
        Batch upsert positions to database.
        
        Args:
            positions: List of position dictionaries
        """
        
        if not positions:
            return
        
        query = """
        INSERT INTO live_positions (
            address, market, position_size, entry_price, liquidation_price,
            margin_used, position_value, unrealized_pnl, return_on_equity,
            leverage_type, leverage_value, leverage_raw_usd, account_value,
            total_margin_used, withdrawable, last_updated
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW())
        ON CONFLICT (address, market)
        DO UPDATE SET
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
            last_updated = NOW()
        """
        
        # Prepare batch data
        batch_data = []
        for pos in positions:
            batch_data.append((
                pos['address'].lower(),  # Ensure lowercase
                pos['market'].upper(),   # Ensure uppercase for market
                pos.get('position_size', 0),
                pos.get('entry_price'),
                pos.get('liquidation_price'),
                pos.get('margin_used'),
                pos.get('position_value', 0),
                pos.get('unrealized_pnl'),
                pos.get('return_on_equity'),
                pos.get('leverage_type', 'cross'),
                pos.get('leverage_value'),
                pos.get('leverage_raw_usd'),
                pos.get('account_value'),
                pos.get('total_margin_used'),
                pos.get('withdrawable')
            ))
        
        async with self.pool.acquire() as conn:
            await conn.executemany(query, batch_data)
            logger.debug(f"Upserted {len(positions)} positions")
    
    async def delete_positions_batch(self, positions: List[Dict[str, str]]):
        """
        Batch delete closed positions.
        
        Args:
            positions: List of dicts with 'address' and 'market' keys
        """
        
        if not positions:
            return
        
        query = "DELETE FROM live_positions WHERE address = $1 AND market = $2"
        
        batch_data = [(pos['address'].lower(), pos['market'].upper()) for pos in positions]
        
        async with self.pool.acquire() as conn:
            await conn.executemany(query, batch_data)
            logger.debug(f"Deleted {len(positions)} closed positions")
    
    async def get_all_addresses(self) -> Dict[str, Set[str]]:
        """
        Get all addresses from database grouped by market.
        
        Returns:
            Dictionary of market -> set of addresses
        """
        
        query = """
        SELECT DISTINCT market, address 
        FROM live_positions 
        WHERE position_value >= $1
        ORDER BY market, address
        """
        
        addresses_by_market = {}
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, self.config.min_position_size_usd)
            
            for row in rows:
                market = row['market']
                address = row['address']
                
                if market not in addresses_by_market:
                    addresses_by_market[market] = set()
                
                addresses_by_market[market].add(address)
        
        return addresses_by_market
    
    async def get_positions(
        self, 
        market: Optional[str] = None,
        min_value: Optional[float] = None
    ) -> List[Dict]:
        """
        Get positions with optional filters.
        
        Args:
            market: Optional market filter
            min_value: Optional minimum position value filter
            
        Returns:
            List of position dictionaries
        """
        
        conditions = []
        params = []
        param_count = 0
        
        if market:
            param_count += 1
            conditions.append(f"market = ${param_count}")
            params.append(market)
        
        if min_value is not None:
            param_count += 1
            conditions.append(f"position_value >= ${param_count}")
            params.append(min_value)
        else:
            param_count += 1
            conditions.append(f"position_value >= ${param_count}")
            params.append(self.config.min_position_size_usd)
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
        SELECT * FROM live_positions
        {where_clause}
        ORDER BY position_value DESC
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        
        query = """
        SELECT 
            COUNT(DISTINCT address) as unique_addresses,
            COUNT(*) as total_positions,
            SUM(position_value) as total_value_usd,
            AVG(position_value) as avg_position_value,
            MAX(position_value) as max_position_value,
            MIN(last_updated) as oldest_update,
            MAX(last_updated) as newest_update
        FROM live_positions
        WHERE position_value >= $1
        """
        
        stats_by_market_query = """
        SELECT 
            market,
            COUNT(DISTINCT address) as addresses,
            COUNT(*) as positions,
            SUM(position_value) as total_value,
            AVG(position_value) as avg_value
        FROM live_positions
        WHERE position_value >= $1
        GROUP BY market
        """
        
        async with self.pool.acquire() as conn:
            # Overall stats
            overall = await conn.fetchrow(query, self.config.min_position_size_usd)
            
            # Per-market stats
            market_rows = await conn.fetch(stats_by_market_query, self.config.min_position_size_usd)
            
            stats = dict(overall)
            stats['by_market'] = {row['market']: dict(row) for row in market_rows}
            
            return stats
    
    async def cleanup_closed_positions(
        self, 
        max_age_hours: int = DatabaseConfig.CLOSED_POSITION_MAX_AGE_HOURS
    ):

        query = """
        DELETE FROM live_positions
        WHERE (position_size = 0 OR position_value = 0)
        AND last_updated < NOW() - INTERVAL '%s hours'
        RETURNING address, market
        """

        async with self.pool.acquire() as conn:
            deleted = await conn.fetch(query, max_age_hours)

            if deleted:
                logger.info(f"Cleaned up {len(deleted)} confirmed closed positions "
                           f"(not updated in {max_age_hours} hours)")
                for row in deleted:
                    logger.debug(f"Removed closed position: {row['address']}/{row['market']}")

            return [dict(row) for row in deleted]

    async def cleanup_stale_positions(
        self, 
        max_age_hours: int = DatabaseConfig.STALE_POSITION_MAX_AGE_HOURS
    ):
        query = """
        DELETE FROM live_positions
        WHERE last_updated < NOW() - INTERVAL '%s hours'
        AND (position_size = 0 OR position_value = 0)
        RETURNING address, market
        """

        async with self.pool.acquire() as conn:
            deleted = await conn.fetch(query, max_age_hours)

            if deleted:
                logger.warning(f"Emergency cleanup: Removed {len(deleted)} stale closed positions "
                              f"(not updated in {max_age_hours} hours)")
                for row in deleted:
                    logger.debug(f"Emergency cleanup: {row['address']}/{row['market']}")

            return [dict(row) for row in deleted]
