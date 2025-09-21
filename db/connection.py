"""Database connection handling."""
import os
import asyncpg


class DatabaseManager:
    """Simple database manager."""
    
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        self.pool = None
        
    async def initialize(self):
        """Initialize database connection pool."""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=2,
            max_size=10
        )
        
    async def close(self):
        """Close database connections."""
        if self.pool:
            await self.pool.close()
