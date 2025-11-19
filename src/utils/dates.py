"""Date utility functions for roll pressure calculations."""

from datetime import datetime, timedelta
from typing import Optional
import pandas as pd


def parse_date(date_input: str | datetime | pd.Timestamp) -> datetime:
    """
    Parse various date formats to datetime object.

    Args:
        date_input: String (YYYY-MM-DD), datetime, or pandas Timestamp

    Returns:
        datetime object
    """
    if isinstance(date_input, datetime):
        return date_input
    elif isinstance(date_input, pd.Timestamp):
        return date_input.to_pydatetime()
    elif isinstance(date_input, str):
        return datetime.strptime(date_input, '%Y-%m-%d')
    else:
        raise ValueError(f"Cannot parse date from type {type(date_input)}")


def days_between(date1: datetime, date2: datetime) -> int:
    """
    Calculate calendar days between two dates.

    Args:
        date1: Start date
        date2: End date

    Returns:
        Number of days (can be negative if date2 < date1)
    """
    delta = date2 - date1
    return delta.days


def add_business_days(start_date: datetime, num_days: int) -> datetime:
    """
    Add business days (Mon-Fri) to a date.

    Args:
        start_date: Starting date
        num_days: Number of business days to add (can be negative)

    Returns:
        Resulting datetime
    """
    current = start_date
    days_added = 0
    direction = 1 if num_days > 0 else -1
    target = abs(num_days)

    while days_added < target:
        current += timedelta(days=direction)
        # 0 = Monday, 6 = Sunday
        if current.weekday() < 5:  # Monday to Friday
            days_added += 1

    return current


def get_last_business_day_of_month(year: int, month: int) -> datetime:
    """
    Get the last business day of a given month.

    Args:
        year: Year
        month: Month (1-12)

    Returns:
        Last business day as datetime
    """
    # Start with last day of month
    if month == 12:
        last_day = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = datetime(year, month + 1, 1) - timedelta(days=1)

    # Go back until we find a weekday
    while last_day.weekday() >= 5:  # Saturday or Sunday
        last_day -= timedelta(days=1)

    return last_day


def format_contract_code(market: str, year: int, month: int) -> str:
    """
    Format futures contract code (e.g., CLF25 for WTI Jan 2025).

    Args:
        market: 'wti' or 'brent'
        year: Full year (e.g., 2025)
        month: Month (1-12)

    Returns:
        Contract code string
    """
    # Month codes for futures
    month_codes = {
        1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
        7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
    }

    prefix = 'CL' if market.lower() == 'wti' else 'BZ'
    month_code = month_codes[month]
    year_code = str(year)[-2:]  # Last 2 digits

    return f"{prefix}{month_code}{year_code}"


def is_business_day(date: datetime) -> bool:
    """
    Check if a date is a business day (Mon-Fri).

    Args:
        date: Date to check

    Returns:
        True if business day, False otherwise
    """
    return date.weekday() < 5
