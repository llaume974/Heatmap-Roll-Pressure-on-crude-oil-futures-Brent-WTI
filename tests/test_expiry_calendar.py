"""Tests for expiry calendar functionality."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.expiry_calendar import ExpiryCalendar, days_to_expiry
from src.utils.dates import days_between, add_business_days, get_last_business_day_of_month, format_contract_code


class TestDateUtils:
    """Test date utility functions."""

    def test_days_between(self):
        """Test days_between calculation."""
        date1 = datetime(2025, 1, 1)
        date2 = datetime(2025, 1, 10)
        assert days_between(date1, date2) == 9

        # Negative case
        assert days_between(date2, date1) == -9

    def test_add_business_days(self):
        """Test business days addition."""
        # Monday Jan 6, 2025
        start = datetime(2025, 1, 6)

        # Add 5 business days (Mon + 5 = Mon next week)
        result = add_business_days(start, 5)
        assert result.weekday() == 0  # Monday
        assert result.day == 13

    def test_add_business_days_negative(self):
        """Test subtracting business days."""
        # Friday Jan 10, 2025
        start = datetime(2025, 1, 10)

        # Subtract 5 business days
        result = add_business_days(start, -5)
        assert result.weekday() == 4  # Friday
        assert result.day == 3

    def test_get_last_business_day_of_month(self):
        """Test last business day calculation."""
        # January 2025 ends on Friday the 31st
        result = get_last_business_day_of_month(2025, 1)
        assert result.day == 31
        assert result.month == 1
        assert result.weekday() < 5

    def test_format_contract_code(self):
        """Test contract code formatting."""
        # January 2025 WTI = CLF25
        assert format_contract_code('wti', 2025, 1) == 'CLF25'

        # March 2025 Brent = BZH25
        assert format_contract_code('brent', 2025, 3) == 'BZH25'

        # December 2026 WTI = CLZ26
        assert format_contract_code('wti', 2026, 12) == 'CLZ26'


class TestExpiryCalendar:
    """Test ExpiryCalendar class."""

    @pytest.fixture
    def calendar(self):
        """Load real calendar for testing."""
        return ExpiryCalendar('calendar/contracts.csv')

    def test_load_calendar(self, calendar):
        """Test calendar loads successfully."""
        assert calendar.contracts_df is not None
        assert len(calendar.contracts_df) > 0
        assert 'expiry_date' in calendar.contracts_df.columns
        assert 'symbol' in calendar.contracts_df.columns
        assert 'contract_code' in calendar.contracts_df.columns

    def test_calendar_has_both_markets(self, calendar):
        """Test calendar includes WTI and Brent."""
        symbols = calendar.contracts_df['symbol'].unique()
        assert 'WTI' in symbols
        assert 'Brent' in symbols

    def test_get_active_contracts(self, calendar):
        """Test getting active contracts."""
        reference = datetime.now()
        active = calendar.get_active_contracts(reference)

        assert len(active) > 0
        # All expiry dates should be >= reference date
        assert (active['expiry_date'] >= reference).all()

    def test_get_front_contract_wti(self, calendar):
        """Test getting WTI front contract."""
        reference = datetime.now()
        front = calendar.get_front_contract('wti', reference)

        assert front is not None
        contract_code, expiry_date = front

        # Contract code should start with CL
        assert contract_code.startswith('CL')
        # Expiry should be in the future
        assert expiry_date >= reference

    def test_get_front_contract_brent(self, calendar):
        """Test getting Brent front contract."""
        reference = datetime.now()
        front = calendar.get_front_contract('brent', reference)

        assert front is not None
        contract_code, expiry_date = front

        # Contract code should start with BZ
        assert contract_code.startswith('BZ')
        # Expiry should be in the future
        assert expiry_date >= reference

    def test_days_to_expiry_positive(self, calendar):
        """Test days to expiry returns positive value."""
        reference = datetime.now()
        days_wti = calendar.days_to_expiry('wti', reference)

        assert days_wti is not None
        assert days_wti >= 0  # Should never be negative

    def test_days_to_expiry_calculation(self, calendar):
        """Test days to expiry calculation accuracy."""
        # Use a fixed reference date for reproducibility
        reference = datetime(2025, 8, 1)

        # Get front contract
        front = calendar.get_front_contract('wti', reference)
        if front:
            _, expiry_date = front
            expected_days = (expiry_date - reference).days

            calculated_days = calendar.days_to_expiry('wti', reference)

            assert calculated_days == max(0, expected_days)

    def test_get_contract_info(self, calendar):
        """Test getting specific contract info."""
        # Get any contract code
        first_contract = calendar.contracts_df.iloc[0]['contract_code']

        info = calendar.get_contract_info(first_contract)

        assert info is not None
        assert 'symbol' in info
        assert 'expiry_date' in info
        assert 'exchange' in info

    def test_convenience_function(self):
        """Test convenience function days_to_expiry."""
        days = days_to_expiry('wti')

        assert days is not None
        assert isinstance(days, int)
        assert days >= 0


class TestCalendarCreation:
    """Test calendar creation functionality."""

    def test_create_default_calendar(self, tmp_path):
        """Test creating a default calendar."""
        calendar_path = tmp_path / "test_contracts.csv"

        calendar = ExpiryCalendar(str(calendar_path))

        # Calendar should be created
        assert calendar_path.exists()
        assert calendar.contracts_df is not None
        assert len(calendar.contracts_df) > 0

        # Should have both WTI and Brent
        symbols = calendar.contracts_df['symbol'].unique()
        assert 'WTI' in symbols
        assert 'Brent' in symbols

    def test_calendar_date_format(self, tmp_path):
        """Test calendar has proper date format."""
        calendar_path = tmp_path / "test_contracts.csv"
        calendar = ExpiryCalendar(str(calendar_path))

        # Expiry dates should be datetime
        assert pd.api.types.is_datetime64_any_dtype(calendar.contracts_df['expiry_date'])


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
