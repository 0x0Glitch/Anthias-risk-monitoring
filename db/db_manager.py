"""Database connection handling and operations."""
import os
import asyncpg
from pathlib import Path
from typing import Dict, List, Optional, Any


class DatabaseManager:
    """Database manager with connection pooling and operations."""
    
    def __init__(self, config=None):
        self.database_url = os.getenv('DATABASE_URL')
        self.pool = None
        self.config = config
        
    async def initialize(self):
        """Initialize database connection pool and schema."""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=2,
            max_size=10,
            command_timeout=30
        )
        
        # Initialize schema
        await self._initialize_schema()
        
    async def _initialize_schema(self):
        """Initialize database schema from SQL file."""
        schema_path = Path(__file__).parent / "schema.sql"
        
        if not schema_path.exists():
            return
            
        async with self.pool.acquire() as conn:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            await conn.execute(schema_sql)
            
    async def upsert_position(self, address: str, market: str, position_data: Dict[str, Any]):
        """Insert or update position data."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO live_positions (
                    address, market, position_size, entry_price, liquidation_price,
                    margin_used, position_value, unrealized_pnl, return_on_equity,
                    leverage_type, leverage_value, leverage_raw_usd,
                    account_value, total_margin_used, withdrawable
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
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
                    last_updated = CURRENT_TIMESTAMP
            """, address, market, 
                position_data.get('position_size'),
                position_data.get('entry_price'),
                position_data.get('liquidation_price'),
                position_data.get('margin_used'),
                position_data.get('position_value'),
                position_data.get('unrealized_pnl'),
                position_data.get('return_on_equity'),
                position_data.get('leverage_type', 'cross'),
                position_data.get('leverage_value'),
                position_data.get('leverage_raw_usd'),
                position_data.get('account_value'),
                position_data.get('total_margin_used'),
                position_data.get('withdrawable')
            )
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        async with self.pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_positions,
                    SUM(position_value) as total_value_usd,
                    COUNT(DISTINCT address) as unique_addresses,
                    COUNT(DISTINCT market) as active_markets
                FROM live_positions
                WHERE position_size != 0
            """)
            return dict(stats) if stats else {}
        
    async def cleanup_closed_positions(self, max_age_hours: int = 24) -> List[str]:
        """Remove positions that have been closed for more than max_age_hours."""
        async with self.pool.acquire() as conn:
            closed_positions = await conn.fetch("""
                DELETE FROM live_positions 
                WHERE position_size = 0 
                AND last_updated < NOW() - INTERVAL '%s hours'
                RETURNING address, market
            """, max_age_hours)
            return [f"{row['address']}:{row['market']}" for row in closed_positions]
        
    async def close(self):
        """Close database connections."""
        if self.pool:
            await self.pool.close()
