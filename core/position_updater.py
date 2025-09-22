"""Position updater for monitoring live positions via API."""
import asyncio
import logging
import os
from typing import Dict, List, Optional, Any, Set
from decimal import Decimal

from ..config.config import MonitorConfig
from .api_client import HyperliquidClient


class PositionUpdater:
    """Updates position data by querying clearinghouseState API."""
    
    def __init__(self, config: MonitorConfig, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        
        # API clients with fallback
        self.primary_client = HyperliquidClient(
            os.getenv('NVN_API_URL', 'http://127.0.0.1:3001/info')
        )
        self.fallback_client = HyperliquidClient(
            os.getenv('PUBLIC_API_URL', 'https://api.hyperliquid.xyz/info')
        )
        
        self.batch_size = int(os.getenv('POSITION_REFRESH_BATCH_SIZE', '50'))
        self.active_markets = set(config.target_markets)
        
    async def start(self):
        """Initialize the position updater."""
        await self.primary_client.start()
        await self.fallback_client.start()
        
    async def stop(self):
        """Stop the position updater."""
        await self.primary_client.close()
        await self.fallback_client.close()
        
    async def update_positions(self, addresses_by_market: Dict[str, Set[str]]) -> int:
        """Update positions for given addresses by market."""
        total_updated = 0
        
        for market, addresses in addresses_by_market.items():
            if market not in self.active_markets:
                continue
                
            # Process addresses in batches
            address_list = list(addresses)
            for i in range(0, len(address_list), self.batch_size):
                batch = address_list[i:i + self.batch_size]
                updated = await self._update_batch(market, batch)
                total_updated += updated
                
                # Small delay between batches to avoid rate limiting
                if i + self.batch_size < len(address_list):
                    await asyncio.sleep(0.1)
                    
        return total_updated
    
    async def _update_batch(self, market: str, addresses: List[str]) -> int:
        """Update a batch of addresses for a specific market."""
        updated_count = 0
        
        # Create tasks for parallel processing
        tasks = []
        for address in addresses:
            task = asyncio.create_task(
                self._update_single_position(address, market)
            )
            tasks.append(task)
            
        # Process results
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Error updating position: {result}")
            elif result:
                updated_count += 1
                
        return updated_count
    
    async def _update_single_position(self, address: str, market: str) -> bool:
        """Update position for a single address/market combination."""
        try:
            # Try primary API first, fallback to public if needed
            position_data = await self._fetch_position_data(address)
            
            if not position_data:
                return False
                
            # Extract relevant position for this market
            market_position = self._extract_market_position(position_data, market)
            
            if market_position:
                # Store in database
                await self.db_manager.upsert_position(address, market, market_position)
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to update position for {address}:{market}: {e}")
            
        return False
    
    async def _fetch_position_data(self, address: str) -> Optional[Dict]:
        """Fetch position data with fallback."""
        try:
            # Try primary API
            return await self.primary_client.get_clearinghouse_state(address)
        except Exception as e:
            self.logger.warning(f"Primary API failed for {address}: {e}")
            
            try:
                # Fallback to public API
                return await self.fallback_client.get_clearinghouse_state(address)
            except Exception as e2:
                self.logger.error(f"Both APIs failed for {address}: {e2}")
                
        return None
    
    def _extract_market_position(self, position_data: Dict, market: str) -> Optional[Dict]:
        """Extract position data for specific market."""
        if not position_data or 'assetPositions' not in position_data:
            return None
            
        asset_positions = position_data.get('assetPositions', [])
        
        for position in asset_positions:
            if position.get('position', {}).get('coin') == market:
                return self._format_position_data(position, position_data)
                
        return None
    
    def _format_position_data(self, position: Dict, full_data: Dict) -> Dict:
        """Format position data for database storage."""
        pos_info = position.get('position', {})
        margin_summary = full_data.get('marginSummary', {})
        
        return {
            'position_size': self._safe_decimal(pos_info.get('szi')),
            'entry_price': self._safe_decimal(pos_info.get('entryPx')),
            'liquidation_price': self._safe_decimal(pos_info.get('liquidationPx')),
            'margin_used': self._safe_decimal(pos_info.get('marginUsed')),
            'position_value': self._calculate_position_value(pos_info),
            'unrealized_pnl': self._safe_decimal(pos_info.get('unrealizedPnl')),
            'return_on_equity': self._safe_decimal(pos_info.get('returnOnEquity')),
            'leverage_type': pos_info.get('leverageType', 'cross'),
            'leverage_value': self._safe_int(pos_info.get('leverage')),
            'leverage_raw_usd': self._safe_decimal(pos_info.get('leverageRawUsd')),
            'account_value': self._safe_decimal(margin_summary.get('accountValue')),
            'total_margin_used': self._safe_decimal(margin_summary.get('totalMarginUsed')),
            'withdrawable': self._safe_decimal(margin_summary.get('withdrawable'))
        }
    
    def _calculate_position_value(self, position: Dict) -> Optional[Decimal]:
        """Calculate position value in USD."""
        try:
            size = self._safe_decimal(position.get('szi'))
            price = self._safe_decimal(position.get('markPx')) or self._safe_decimal(position.get('entryPx'))
            
            if size and price:
                return abs(size) * price
                
        except Exception:
            pass
            
        return None
    
    def _safe_decimal(self, value) -> Optional[Decimal]:
        """Safely convert value to Decimal."""
        if value is None or value == '':
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None
    
    def _safe_int(self, value) -> Optional[int]:
        """Safely convert value to int."""
        if value is None:
            return None
        try:
            return int(float(value))
        except Exception:
            return None
