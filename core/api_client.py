"""API client for Hyperliquid endpoints."""
import aiohttp
import asyncio
import logging
from typing import Dict, Optional, Any
import time


class HyperliquidClient:
    """Client for Hyperliquid API calls with retry logic and error handling."""
    
    def __init__(self, api_url: str, max_retries: int = 3, timeout: int = 30):
        self.api_url = api_url
        self.session = None
        self.max_retries = max_retries
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms between requests
        
    async def start(self):
        """Start HTTP session with timeout configuration."""
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'Content-Type': 'application/json'}
        )
        
    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None
        
    async def get_clearinghouse_state(self, address: str) -> Optional[Dict[str, Any]]:
        """Get clearinghouse state for an address with retry logic."""
        if not self.session:
            await self.start()
            
        data = {
            "type": "clearinghouseState",
            "user": address
        }
        
        for attempt in range(self.max_retries):
            try:
                # Rate limiting
                await self._rate_limit()
                
                async with self.session.post(self.api_url, json=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
                    elif response.status == 429:  # Rate limited
                        wait_time = (attempt + 1) * 2
                        self.logger.warning(f"Rate limited, waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        self.logger.warning(f"HTTP {response.status} for {address}")
                        if attempt == self.max_retries - 1:
                            return None
                        
            except aiohttp.ClientError as e:
                self.logger.warning(f"Client error for {address}: {e}")
                if attempt == self.max_retries - 1:
                    return None
                await asyncio.sleep((attempt + 1) * 1.0)
                
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout for {address}")
                if attempt == self.max_retries - 1:
                    return None
                await asyncio.sleep((attempt + 1) * 1.0)
                
            except Exception as e:
                self.logger.error(f"Unexpected error for {address}: {e}")
                if attempt == self.max_retries - 1:
                    return None
                await asyncio.sleep((attempt + 1) * 1.0)
        
        return None
    
    async def get_exchange_status(self) -> Optional[Dict[str, Any]]:
        """Get exchange status for health checks."""
        if not self.session:
            await self.start()
            
        data = {"type": "exchangeStatus"}
        
        try:
            await self._rate_limit()
            async with self.session.post(self.api_url, json=data) as response:
                if response.status == 200:
                    return await response.json()
                    
        except Exception as e:
            self.logger.debug(f"Exchange status check failed: {e}")
            
        return None
    
    async def _rate_limit(self):
        """Implement rate limiting between requests."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - elapsed)
            
        self.last_request_time = time.time()
        
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        return {
            'api_url': self.api_url,
            'max_retries': self.max_retries,
            'timeout': self.timeout,
            'session_active': self.session is not None and not self.session.closed
        }
