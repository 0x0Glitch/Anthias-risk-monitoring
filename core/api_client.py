"""API client for Hyperliquid endpoints."""
import aiohttp
import asyncio


class HyperliquidClient:
    """Client for Hyperliquid API calls."""
    
    def __init__(self, api_url: str):
        self.api_url = api_url
        self.session = None
        
    async def start(self):
        """Start HTTP session."""
        self.session = aiohttp.ClientSession()
        
    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
        
    async def get_clearinghouse_state(self, address: str):
        """Get clearinghouse state for an address."""
        if not self.session:
            await self.start()
            
        data = {
            "type": "clearinghouseState",
            "user": address
        }
        
        async with self.session.post(self.api_url, json=data) as response:
            return await response.json()
