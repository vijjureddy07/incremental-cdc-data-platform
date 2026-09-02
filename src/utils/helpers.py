"""Shared utility functions for timestamp formatting, decimals, and filesystem."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


def format_iso_timestamp(dt: datetime) -> str:
    """Format a datetime object into a standardized ISO 8601 UTC string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso_timestamp(ts_str: str) -> datetime:
    """Parse an ISO 8601 timestamp string into a timezone-aware UTC datetime."""
    clean_str = ts_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(clean_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def format_iso_date(d: date) -> str:
    """Format a date object into ISO 8601 YYYY-MM-DD string."""
    return d.isoformat()


def parse_iso_date(d_str: str) -> date:
    """Parse a YYYY-MM-DD string into a date object."""
    return date.fromisoformat(d_str)


def format_decimal(val: Decimal | float | int | str, places: int = 2) -> str:
    """Format a numeric value into a fixed-decimal string."""
    dec = Decimal(str(val))
    return f"{dec:.{places}f}"


def ensure_dir(dir_path: Path | str) -> Path:
    """Ensure a directory exists and return the Path object."""
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def safe_json_default(obj: Any) -> Any:
    """Fallback JSON serializer for custom Python types."""
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return format_iso_timestamp(obj)
    if isinstance(obj, date):
        return format_iso_date(obj)
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
