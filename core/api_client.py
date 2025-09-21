"""API client for Hyperliquid endpoints."""


class HyperliquidClient:
    """Client for Hyperliquid API calls."""
    
    def __init__(self, api_url: str):
        self.api_url = api_url
        
    async def get_clearinghouse_state(self, address: str):
        """Get clearinghouse state for an address."""
        # TODO: Implement HTTP request
        pass
