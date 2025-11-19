"""
Expiry calendar management for WTI and Brent futures contracts.

Handles loading contract expiry dates and calculating days to expiry.
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
from loguru import logger

from ..utils.dates import parse_date, days_between, add_business_days, get_last_business_day_of_month, format_contract_code


class ExpiryCalendar:
    """Manage futures contract expiry dates and calculations."""

    def __init__(self, calendar_path: str = "calendar/contracts.csv"):
        """
        Initialize expiry calendar.

        Args:
            calendar_path: Path to contracts CSV file
        """
        self.calendar_path = Path(calendar_path)
        self.contracts_df: Optional[pd.DataFrame] = None

        if self.calendar_path.exists():
            self.load_calendar()
        else:
            logger.warning(f"Calendar file not found: {calendar_path}. Will create default.")
            self.create_default_calendar()

    def load_calendar(self):
        """Load contract calendar from CSV."""
        try:
            self.contracts_df = pd.read_csv(self.calendar_path)
            self.contracts_df['expiry_date'] = pd.to_datetime(self.contracts_df['expiry_date'])
            logger.info(f"Loaded {len(self.contracts_df)} contracts from {self.calendar_path}")
        except Exception as e:
            logger.error(f"Error loading calendar: {e}")
            raise

    def save_calendar(self):
        """Save contract calendar to CSV."""
        if self.contracts_df is not None:
            self.calendar_path.parent.mkdir(parents=True, exist_ok=True)
            self.contracts_df.to_csv(self.calendar_path, index=False)
            logger.info(f"Saved calendar to {self.calendar_path}")

    def create_default_calendar(self, months_ahead: int = 18):
        """
        Create default calendar with approximate expiry dates.

        Args:
            months_ahead: Number of months to generate ahead

        Note:
            This creates approximate dates. Users should manually verify/edit
            actual expiry dates in the CSV file.
        """
        contracts = []
        today = datetime.now()

        for month_offset in range(-2, months_ahead + 1):  # Include 2 past months
            target_date = today + timedelta(days=30 * month_offset)
            year = target_date.year
            month = target_date.month

            # WTI NYMEX: ~3rd business day before 25th of prior month
            # For delivery month M, last trading is usually in M-1
            if month == 1:
                wti_month = 12
                wti_year = year - 1
            else:
                wti_month = month - 1
                wti_year = year

            # Start with 25th of prior month
            wti_base = datetime(wti_year, wti_month, 25)
            # Go back 3 business days
            wti_expiry = add_business_days(wti_base, -3)

            contracts.append({
                'symbol': 'WTI',
                'contract_code': format_contract_code('wti', year, month),
                'expiry_date': wti_expiry.strftime('%Y-%m-%d'),
                'exchange': 'NYMEX',
                'delivery_month': f"{year}-{month:02d}"
            })

            # Brent ICE: Last business day of 2 months before delivery
            # For delivery month M, last trading is end of M-2
            if month <= 2:
                brent_month = month + 10
                brent_year = year - 1
            else:
                brent_month = month - 2
                brent_year = year

            brent_expiry = get_last_business_day_of_month(brent_year, brent_month)

            contracts.append({
                'symbol': 'Brent',
                'contract_code': format_contract_code('brent', year, month),
                'expiry_date': brent_expiry.strftime('%Y-%m-%d'),
                'exchange': 'ICE',
                'delivery_month': f"{year}-{month:02d}"
            })

        self.contracts_df = pd.DataFrame(contracts)
        self.contracts_df['expiry_date'] = pd.to_datetime(self.contracts_df['expiry_date'])

        # Save to file
        self.save_calendar()

        logger.info(f"Created default calendar with {len(self.contracts_df)} contracts")
        logger.warning("⚠️  Default expiry dates are approximations. Please verify and edit calendar/contracts.csv")

    def get_active_contracts(self, reference_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Get contracts that haven't expired yet.

        Args:
            reference_date: Reference date (default: today)

        Returns:
            DataFrame of active contracts
        """
        if reference_date is None:
            reference_date = datetime.now()

        if self.contracts_df is None:
            raise ValueError("Calendar not loaded")

        active = self.contracts_df[self.contracts_df['expiry_date'] >= reference_date].copy()
        return active.sort_values('expiry_date')

    def get_front_contract(self, market: str, reference_date: Optional[datetime] = None) -> Optional[Tuple[str, datetime]]:
        """
        Get the front-month (nearest expiry) contract for a market.

        Args:
            market: 'wti' or 'brent' (case-insensitive)
            reference_date: Reference date (default: today)

        Returns:
            Tuple of (contract_code, expiry_date) or None if not found
        """
        if reference_date is None:
            reference_date = datetime.now()

        market_name = 'WTI' if market.lower() == 'wti' else 'Brent'

        active = self.get_active_contracts(reference_date)
        market_contracts = active[active['symbol'] == market_name]

        if len(market_contracts) == 0:
            logger.warning(f"No active contracts found for {market_name}")
            return None

        front = market_contracts.iloc[0]
        return front['contract_code'], front['expiry_date']

    def days_to_expiry(self, market: str, reference_date: Optional[datetime] = None) -> Optional[int]:
        """
        Calculate days to expiry for front-month contract.

        Args:
            market: 'wti' or 'brent'
            reference_date: Reference date (default: today)

        Returns:
            Number of days to expiry, or None if not found
        """
        if reference_date is None:
            reference_date = datetime.now()

        front = self.get_front_contract(market, reference_date)

        if front is None:
            return None

        _, expiry_date = front
        days = days_between(reference_date, expiry_date)

        return max(0, days)  # Return 0 if already expired (shouldn't happen with get_front_contract)

    def get_contract_info(self, contract_code: str) -> Optional[dict]:
        """
        Get information for a specific contract.

        Args:
            contract_code: Contract code (e.g., 'CLF25')

        Returns:
            Contract info dict or None
        """
        if self.contracts_df is None:
            raise ValueError("Calendar not loaded")

        match = self.contracts_df[self.contracts_df['contract_code'] == contract_code]

        if len(match) == 0:
            return None

        return match.iloc[0].to_dict()


def days_to_expiry(market: str, reference_date: Optional[datetime] = None, calendar_path: str = "calendar/contracts.csv") -> Optional[int]:
    """
    Convenience function to calculate days to expiry.

    Args:
        market: 'wti' or 'brent'
        reference_date: Reference date (default: today)
        calendar_path: Path to calendar CSV

    Returns:
        Days to expiry or None
    """
    calendar = ExpiryCalendar(calendar_path)
    return calendar.days_to_expiry(market, reference_date)
