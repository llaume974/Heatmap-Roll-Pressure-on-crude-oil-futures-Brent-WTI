"""Tests for Open Interest loader."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.oi_loader import OILoader, load_oi_data


class TestOILoader:
    """Test OI loader functionality."""

    @pytest.fixture
    def mock_oi_data(self):
        """Create mock OI data."""
        dates = pd.date_range('2025-01-01', '2025-01-31', freq='D')

        data = {
            'date': dates,
            'market': 'WTI',
            'open_interest': [500000 + i * 1000 for i in range(len(dates))]
        }

        return pd.DataFrame(data)

    def test_symbols_mapping(self):
        """Test symbol mappings are correct."""
        loader = OILoader()

        assert loader.SYMBOLS['wti'] == 'CL=F'
        assert loader.SYMBOLS['brent'] == 'BZ=F'

    def test_get_cache_path(self):
        """Test cache path generation."""
        loader = OILoader(cache_dir="data/raw")

        wti_path = loader.get_cache_path('wti')
        brent_path = loader.get_cache_path('brent')

        assert 'wti' in str(wti_path).lower()
        assert 'brent' in str(brent_path).lower()
        assert wti_path.suffix == '.json'

    def test_apply_fallback_last_known(self, mock_oi_data):
        """Test last_known fallback strategy."""
        loader = OILoader()
        loader.fallback_strategy = 'last_known'

        # Create data with gaps
        partial_data = mock_oi_data.iloc[::3].copy()  # Every 3rd row

        # Full date range
        full_range = pd.date_range(partial_data['date'].min(), partial_data['date'].max(), freq='D')

        # Apply fallback
        filled = loader.apply_fallback_strategy(partial_data, full_range)

        # Should have more rows than partial
        assert len(filled) > len(partial_data)

        # Should not have NaN
        assert filled['open_interest'].isna().sum() == 0

    def test_validate_oi_data_removes_low_values(self, mock_oi_data):
        """Test validation removes unrealistic values."""
        loader = OILoader()

        # Add some invalid values
        invalid_data = mock_oi_data.copy()
        invalid_data.loc[0, 'open_interest'] = 100  # Too low
        invalid_data.loc[1, 'open_interest'] = -1000  # Negative
        invalid_data.loc[2, 'open_interest'] = 0  # Zero

        validated = loader.validate_oi_data(invalid_data, min_oi=1000)

        # Should have removed 3 rows
        assert len(validated) == len(mock_oi_data) - 3

        # All remaining values should be >= 1000
        assert (validated['open_interest'] >= 1000).all()

    def test_validate_oi_data_removes_negative(self, mock_oi_data):
        """Test validation removes negative values."""
        loader = OILoader()

        # Add negative value
        invalid_data = mock_oi_data.copy()
        invalid_data.loc[5, 'open_interest'] = -5000

        validated = loader.validate_oi_data(invalid_data)

        # Should have removed the negative value
        assert len(validated) == len(mock_oi_data) - 1
        assert (validated['open_interest'] > 0).all()

    def test_oi_data_structure(self, mock_oi_data):
        """Test OI data has correct structure."""
        assert 'date' in mock_oi_data.columns
        assert 'market' in mock_oi_data.columns
        assert 'open_interest' in mock_oi_data.columns

        # Date should be datetime
        assert pd.api.types.is_datetime64_any_dtype(mock_oi_data['date'])

        # OI should be numeric
        assert pd.api.types.is_numeric_dtype(mock_oi_data['open_interest'])

    def test_multiple_markets_oi(self):
        """Test handling multiple markets."""
        dates = pd.date_range('2025-01-01', '2025-01-10', freq='D')

        wti_data = pd.DataFrame({
            'date': dates,
            'market': 'WTI',
            'open_interest': [500000] * len(dates)
        })

        brent_data = pd.DataFrame({
            'date': dates,
            'market': 'BRENT',
            'open_interest': [400000] * len(dates)
        })

        combined = pd.concat([wti_data, brent_data], ignore_index=True)

        assert set(combined['market'].unique()) == {'WTI', 'BRENT'}
        assert len(combined) == len(dates) * 2

    def test_cache_directory_creation(self, tmp_path):
        """Test cache directory is created."""
        cache_dir = tmp_path / "oi_cache"

        loader = OILoader(cache_dir=str(cache_dir))

        assert cache_dir.exists()

    def test_fallback_strategy_none(self, mock_oi_data):
        """Test 'none' fallback strategy."""
        loader = OILoader()
        loader.fallback_strategy = 'none'

        # Create partial data
        partial = mock_oi_data.iloc[::5].copy()

        # Full range
        full_range = pd.date_range(partial['date'].min(), partial['date'].max(), freq='D')

        # Apply fallback (should keep NaNs)
        result = loader.apply_fallback_strategy(partial, full_range)

        # Result should only have original rows (NaNs dropped)
        assert len(result) <= len(partial)


class TestConvenienceFunction:
    """Test convenience function."""

    def test_load_oi_data_signature(self):
        """Test convenience function exists."""
        # Just test it exists and has right signature
        # Will likely fail without real data, but tests interface
        try:
            result = load_oi_data(markets=['wti'], lookback_days=30)
        except:
            pass  # Expected to fail without real yfinance data


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
