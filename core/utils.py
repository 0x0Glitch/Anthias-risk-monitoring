"""Utility functions for the monitoring system."""
import json
import msgpack
from typing import Any, Optional, Union
from decimal import Decimal
import logging


def format_currency(amount: Optional[Union[float, Decimal, str]], precision: int = 2) -> str:
    """Format currency amount with proper formatting."""
    if amount is None:
        return "N/A"
    
    try:
        value = float(amount)
        if abs(value) >= 1_000_000:
            return f"${value/1_000_000:.1f}M"
        elif abs(value) >= 1_000:
            return f"${value/1_000:.1f}K"
        else:
            return f"${value:.{precision}f}"
    except (ValueError, TypeError):
        return "N/A"


def calculate_position_value(size: Optional[Union[float, str]], price: Optional[Union[float, str]]) -> Optional[Decimal]:
    """Calculate position value from size and price."""
    try:
        if size is None or price is None:
            return None
            
        size_val = Decimal(str(size))
        price_val = Decimal(str(price))
        
        return abs(size_val) * price_val
        
    except (ValueError, TypeError, InvalidOperation):
        return None


def safe_float_parse(value: Any, default: float = 0.0) -> float:
    """Safely parse a value to float with default fallback."""
    if value is None:
        return default
        
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_decimal_parse(value: Any) -> Optional[Decimal]:
    """Safely parse a value to Decimal."""
    if value is None or value == '':
        return None
        
    try:
        return Decimal(str(value))
    except (ValueError, TypeError, InvalidOperation):
        return None


def serialize_data(data: Any, format: str = 'json') -> bytes:
    """Serialize data to bytes using specified format."""
    if format == 'json':
        return json.dumps(data, default=str).encode('utf-8')
    elif format == 'msgpack':
        return msgpack.packb(data, default=str)
    else:
        raise ValueError(f"Unsupported format: {format}")


def deserialize_data(data: bytes, format: str = 'json') -> Any:
    """Deserialize bytes to data using specified format."""
    if format == 'json':
        return json.loads(data.decode('utf-8'))
    elif format == 'msgpack':
        return msgpack.unpackb(data, raw=False)
    else:
        raise ValueError(f"Unsupported format: {format}")


def validate_address(address: str) -> bool:
    """Validate Ethereum address format."""
    if not isinstance(address, str):
        return False
        
    # Basic validation - 42 characters, starts with 0x, hex characters
    if len(address) != 42:
        return False
        
    if not address.startswith('0x'):
        return False
        
    try:
        int(address[2:], 16)
        return True
    except ValueError:
        return False


def batch_list(items: list, batch_size: int):
    """Split a list into batches of specified size."""
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def log_execution_time(func_name: str):
    """Decorator to log function execution time."""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                logging.getLogger('performance').info(f"{func_name}: {duration_ms:.2f}ms")
                return result
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                logging.getLogger('performance').error(f"{func_name} failed after {duration_ms:.2f}ms: {e}")
                raise
                
        def sync_wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                logging.getLogger('performance').info(f"{func_name}: {duration_ms:.2f}ms")
                return result
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                logging.getLogger('performance').error(f"{func_name} failed after {duration_ms:.2f}ms: {e}")
                raise
                
        # Return appropriate wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator
