"""
Utility functions for Hyperliquid Position Monitoring System.
Extracts common helper functions used across multiple modules.
"""
from typing import Any, Optional

from config.constants import ADDRESS_LENGTH, ADDRESS_PREFIX, HEX_CHARS


def is_ethereum_address(address: str) -> bool:
    """
    Check if a string is a valid Ethereum address (0x followed by 40 hex chars).
    Extracted from snapshot_processor.py to avoid duplication.
    """
    if not isinstance(address, str):
        return False
    address = address.strip()
    return (
        len(address) == ADDRESS_LENGTH and
        address[:2].lower() == ADDRESS_PREFIX and
        all(c in HEX_CHARS for c in address[2:])
    )


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """
    Safely convert value to float.
    Used in position_updater.py for API response parsing.
    """
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default