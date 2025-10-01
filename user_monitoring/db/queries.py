"""
SQL Query Manager for User Metrics Database Operations.
Provides descriptive method names for all database operations.
"""
import asyncpg
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class UserMetricsQueries:
    """
    Manages all database queries with descriptive method names.
    Replaces inline SQL with clean, readable method calls.
    """
    
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
    
    def _get_table_name(self, token: str) -> str:
        """Get the formatted table name for a token."""
        # Sanitize token name to prevent SQL injection
        import re
        sanitized_token = re.sub(r'[^a-zA-Z0-9_]', '', token.lower())
        return f"user_metrics.{sanitized_token}_live_positions"
    
    async def upsert_positions(self, token: str, positions: List[Dict[str, Any]]) -> None:
        """
        Upsert position data for a specific token.
        2-3 words: upsert_positions
        """
        if not positions:
            return
            
        table_name = self._get_table_name(token)
        
        query = f"""
        INSERT INTO {table_name} (
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
                pos['address'].lower(),
                pos['market'].upper(),
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
        
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany(query, batch_data)
        except Exception as e:
            logger.error(f"Failed to upsert positions for {token}: {e}")
            raise
    
    async def remove_positions(self, token: str, positions: List[Dict[str, str]]) -> None:
        """
        Remove closed positions for a specific token.
        2-3 words: remove_positions
        """
        if not positions:
            return
            
        table_name = self._get_table_name(token)
        query = f"DELETE FROM {table_name} WHERE address = $1 AND market = $2"
        
        batch_data = [(pos['address'].lower(), pos['market'].upper()) for pos in positions]
        
        async with self.pool.acquire() as conn:
            await conn.executemany(query, batch_data)
    
    async def bulk_remove_addresses(self, token: str, addresses: List[str]) -> None:
        """
        Bulk remove all positions for specific addresses.
        2-3 words: bulk_remove_addresses
        """
        if not addresses:
            return
            
        table_name = self._get_table_name(token)
        
        async with self.pool.acquire() as conn:
            # Process in chunks of 100 for efficiency
            for i in range(0, len(addresses), 100):
                chunk = addresses[i:i+100]
                await conn.execute(
                    f"DELETE FROM {table_name} WHERE address = ANY($1)",
                    chunk
                )
    
    async def get_active_addresses(self, token: str, min_value: float) -> Dict[str, set]:
        """
        Get addresses with active positions grouped by market.
        2-3 words: get_active_addresses
        """
        table_name = self._get_table_name(token)
        
        query = f"""
        SELECT DISTINCT market, address 
        FROM {table_name} 
        WHERE position_value >= $1
        ORDER BY market, address
        """
        
        addresses_by_market = {}
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, min_value)
            
            for row in rows:
                market = row['market']
                address = row['address']
                
                if market not in addresses_by_market:
                    addresses_by_market[market] = set()
                
                addresses_by_market[market].add(address)
        
        return addresses_by_market
    
    async def get_filtered_positions(
        self, 
        token: str, 
        market: Optional[str] = None,
        min_value: Optional[float] = None,
        default_min_value: float = 0
    ) -> List[Dict]:
        """
        Get positions with optional market and value filters.
        2-3 words: get_filtered_positions
        """
        table_name = self._get_table_name(token)
        
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
            params.append(default_min_value)
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
        SELECT * FROM {table_name}
        {where_clause}
        ORDER BY position_value DESC
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]
    
    async def calculate_overall_stats(self, token: str, min_value: float) -> Dict[str, Any]:
        """
        Calculate overall statistics for a token.
        2-3 words: calculate_overall_stats
        """
        table_name = self._get_table_name(token)
        
        query = f"""
        SELECT 
            COUNT(DISTINCT address) as unique_addresses,
            COUNT(*) as total_positions,
            SUM(position_value) as total_value_usd,
            AVG(position_value) as avg_position_value,
            MAX(position_value) as max_position_value,
            MIN(last_updated) as oldest_update,
            MAX(last_updated) as newest_update
        FROM {table_name}
        WHERE position_value >= $1
        """
        
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(query, min_value)
            return dict(result) if result else {}
    
    async def calculate_market_stats(self, token: str, min_value: float) -> Dict[str, Dict]:
        """
        Calculate per-market statistics for a token.
        2-3 words: calculate_market_stats
        """
        table_name = self._get_table_name(token)
        
        query = f"""
        SELECT 
            market,
            COUNT(DISTINCT address) as addresses,
            COUNT(*) as positions,
            SUM(position_value) as total_value,
            AVG(position_value) as avg_value
        FROM {table_name}
        WHERE position_value >= $1
        GROUP BY market
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, min_value)
            return {row['market']: dict(row) for row in rows}
    
    async def cleanup_closed_positions(self, token: str, max_age_hours: int) -> List[Dict]:
        """
        Remove old closed positions.
        2-3 words: cleanup_closed_positions
        """
        table_name = self._get_table_name(token)
        
        query = f"""
        DELETE FROM {table_name}
        WHERE (position_size = 0 OR position_value = 0)
        AND last_updated < NOW() - INTERVAL '{max_age_hours} hours'
        RETURNING address, market
        """
        
        async with self.pool.acquire() as conn:
            deleted = await conn.fetch(query)
            return [dict(row) for row in deleted]
    
    async def cleanup_stale_positions(self, token: str, max_age_hours: int) -> List[Dict]:
        """
        Emergency cleanup of very old positions.
        2-3 words: cleanup_stale_positions
        """
        table_name = self._get_table_name(token)
        
        query = f"""
        DELETE FROM {table_name}
        WHERE last_updated < NOW() - INTERVAL '{max_age_hours} hours'
        AND (position_size = 0 OR position_value = 0)
        RETURNING address, market
        """
        
        async with self.pool.acquire() as conn:
            deleted = await conn.fetch(query)
            return [dict(row) for row in deleted]
    
    async def verify_table_exists(self, token: str) -> bool:
        """
        Check if the positions table exists for a token.
        2-3 words: verify_table_exists
        """
        table_name = f"{token.lower()}_live_positions"
        
        query = """
        SELECT COUNT(*) as col_count 
        FROM information_schema.columns 
        WHERE table_schema = 'user_metrics' AND table_name = $1
        """
        
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(query, table_name)
            return result['col_count'] > 0
    
    async def create_token_table(self, token: str) -> None:
        """
        Create a new positions table for a token.
        2-3 words: create_token_table
        """
        query = "SELECT user_metrics.create_token_positions_table($1)"
        
        async with self.pool.acquire() as conn:
            await conn.execute(query, token.lower())
