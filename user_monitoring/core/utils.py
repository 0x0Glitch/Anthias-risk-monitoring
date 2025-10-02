"""Utility functions for Hyperliquid Position Monitoring System."""
from typing import Any, Optional

from config.constants import ADDRESS_LENGTH, ADDRESS_PREFIX, HEX_CHARS


def is_ethereum_address(address: str) -> bool:
    if not isinstance(address, str):
        return False
    address = address.strip()
    return (
        len(address) == ADDRESS_LENGTH and
        address[:2].lower() == ADDRESS_PREFIX and
        all(c in HEX_CHARS for c in address[2:])
    )


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default