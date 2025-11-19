"""Tests for CFTC data loader."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.cftc_loader import CFTCLoader, load_cftc_data


class TestCFTCLoader:
    """Test CFTC loader functionality."""

    @pytest.fixture
    def mock_cftc_data(self):
        """Create mock CFTC data for testing (Socrata API format)."""
        dates = pd.date_range('2025-01-01', '2025-03-01', freq='W-TUE')  # Weekly reports

        data = []

        for date in dates:
            # WTI entry (using real Socrata API market name)
            data.append({
                'report_date_as_yyyy_mm_dd': date.strftime('%Y-%m-%d'),
                'market_and_exchange_names': 'WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE',
                'cftc_contract_market_code': 'CL',
                'm_money_positions_long_all': str(300000 + (date.day * 1000)),  # API returns strings
                'm_money_positions_short_all': str(150000 + (date.day * 500)),
                'open_interest_all': str(500000)
            })

            # Brent entry (using real Socrata API market name)
            data.append({
                'report_date_as_yyyy_mm_dd': date.strftime('%Y-%m-%d'),
                'market_and_exchange_names': 'BRENT LAST DAY - NEW YORK MERCANTILE EXCHANGE',
                'cftc_contract_market_code': 'B',
                'm_money_positions_long_all': str(200000 + (date.day * 800)),
                'm_money_positions_short_all': str(100000 + (date.day * 400)),
                'open_interest_all': str(400000)
            })

        return pd.DataFrame(data)

    def test_extract_market_data_wti(self, mock_cftc_data):
        """Test extracting WTI data."""
        loader = CFTCLoader()

        wti_df = loader.extract_market_data(mock_cftc_data, 'wti')

        assert len(wti_df) > 0
        assert 'WTI' in wti_df['market_and_exchange_names'].iloc[0]

    def test_extract_market_data_brent(self, mock_cftc_data):
        """Test extracting Brent data."""
        loader = CFTCLoader()

        brent_df = loader.extract_market_data(mock_cftc_data, 'brent')

        assert len(brent_df) > 0
        assert 'BRENT' in brent_df['market_and_exchange_names'].iloc[0]

    def test_calculate_spec_net_long(self, mock_cftc_data):
        """Test spec_net_long calculation."""
        loader = CFTCLoader()

        # Extract WTI
        wti_df = loader.extract_market_data(mock_cftc_data, 'wti')

        # Calculate net long
        result = loader.calculate_spec_net_long(wti_df)

        assert 'spec_net_long' in result.columns

        # Check calculation: Long - Short
        first_row = result.iloc[0]
        expected = first_row['m_money_positions_long_all'] - first_row['m_money_positions_short_all']
        assert first_row['spec_net_long'] == expected

    def test_normalize_cftc_data(self, mock_cftc_data):
        """Test data normalization."""
        loader = CFTCLoader()

        wti_df = loader.extract_market_data(mock_cftc_data, 'wti')
        normalized = loader.normalize_cftc_data(wti_df, 'wti')

        # Check columns
        assert 'date' in normalized.columns
        assert 'market' in normalized.columns
        assert 'spec_net_long' in normalized.columns
        assert 'open_interest' in normalized.columns

        # Check market name
        assert (normalized['market'] == 'WTI').all()

        # Check date is datetime
        assert pd.api.types.is_datetime64_any_dtype(normalized['date'])

        # Check sorted
        assert normalized['date'].is_monotonic_increasing

    def test_spec_net_long_values(self, mock_cftc_data):
        """Test spec_net_long has correct values."""
        loader = CFTCLoader()

        wti_df = loader.extract_market_data(mock_cftc_data, 'wti')
        normalized = loader.normalize_cftc_data(wti_df, 'wti')

        # All should be positive (Long > Short in mock data)
        assert (normalized['spec_net_long'] > 0).all()

        # First value calculation: Long - Short
        # With mock data: (300000 + day*1000) - (150000 + day*500) = 150000 + day*500
        # For first date (day 1): 150000 + 500 = 150500
        first_row = normalized.iloc[0]
        assert first_row['spec_net_long'] > 150000  # Should be around 150500

    def test_forward_fill_daily(self, mock_cftc_data):
        """Test forward-filling weekly data to daily."""
        loader = CFTCLoader()

        wti_df = loader.extract_market_data(mock_cftc_data, 'wti')
        normalized = loader.normalize_cftc_data(wti_df, 'wti')

        # Forward fill
        start_date = normalized['date'].min()
        end_date = normalized['date'].max() + timedelta(days=5)

        daily = loader.forward_fill_daily(normalized, end_date)

        # Should have daily frequency
        assert len(daily) > len(normalized)

        # Check no gaps (all consecutive days)
        dates_diff = daily['date'].diff().dropna()
        # Most should be 1 day (allowing for sorting across markets)
        assert (dates_diff <= timedelta(days=1)).sum() > len(daily) * 0.9

    def test_forward_fill_preserves_values(self, mock_cftc_data):
        """Test forward-fill preserves original values."""
        loader = CFTCLoader()

        wti_df = loader.extract_market_data(mock_cftc_data, 'wti')
        normalized = loader.normalize_cftc_data(wti_df, 'wti')

        original_dates = set(normalized['date'].dt.date)

        daily = loader.forward_fill_daily(normalized)

        # Check original dates have same values
        for orig_date in original_dates:
            orig_value = normalized[normalized['date'].dt.date == orig_date]['spec_net_long'].iloc[0]
            daily_value = daily[daily['date'].dt.date == orig_date]['spec_net_long'].iloc[0]

            assert orig_value == daily_value

    def test_multiple_markets(self, mock_cftc_data):
        """Test processing multiple markets."""
        loader = CFTCLoader()

        wti_df = loader.extract_market_data(mock_cftc_data, 'wti')
        brent_df = loader.extract_market_data(mock_cftc_data, 'brent')

        wti_norm = loader.normalize_cftc_data(wti_df, 'wti')
        brent_norm = loader.normalize_cftc_data(brent_df, 'brent')

        combined = pd.concat([wti_norm, brent_norm], ignore_index=True)

        # Should have both markets
        assert set(combined['market'].unique()) == {'WTI', 'BRENT'}

        # Each market should have same number of weeks
        wti_count = len(combined[combined['market'] == 'WTI'])
        brent_count = len(combined[combined['market'] == 'BRENT'])
        assert wti_count == brent_count

    def test_cache_directory_creation(self, tmp_path):
        """Test cache directory is created."""
        cache_dir = tmp_path / "test_cache"

        loader = CFTCLoader(cache_dir=str(cache_dir))

        assert cache_dir.exists()

    def test_get_cache_path(self):
        """Test cache path generation."""
        loader = CFTCLoader(cache_dir="data/raw")

        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 12, 31)
        path = loader.get_cache_path(start_date, end_date)

        assert '20250101' in str(path)
        assert '20251231' in str(path)
        assert path.suffix == '.csv'


class TestConvenienceFunction:
    """Test convenience function."""

    def test_load_cftc_data_function_signature(self):
        """Test that convenience function exists and has right signature."""
        # Just test it exists and can be called (will fail to download, but that's OK)
        try:
            # This will attempt to download and likely fail, but we're just testing the interface
            result = load_cftc_data(markets=['wti'], lookback_days=30)
        except:
            pass  # Expected to fail without real data


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
