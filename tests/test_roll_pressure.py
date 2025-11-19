"""Tests for roll pressure calculation."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.features.roll_pressure import RollPressureCalculator


class TestRollPressureCalculator:
    """Test roll pressure calculation functionality."""

    @pytest.fixture
    def mock_config(self):
        """Mock configuration."""
        return {
            'markets': ['wti', 'brent'],
            'calculation': {
                'min_value': 0.0,
                'max_value': 2.0,
                'min_open_interest': 1000
            },
            'alert': {
                'red_threshold': 0.65,
                'days_threshold': 2
            },
            'paths': {
                'calendar': 'calendar/contracts.csv'
            }
        }

    @pytest.fixture
    def mock_cftc_data(self):
        """Create mock CFTC data."""
        dates = pd.date_range('2025-08-01', '2025-08-10', freq='D')

        data = []
        for date in dates:
            data.append({'date': date, 'market': 'WTI', 'spec_net_long': 150000})
            data.append({'date': date, 'market': 'BRENT', 'spec_net_long': 100000})

        return pd.DataFrame(data)

    @pytest.fixture
    def mock_oi_data(self):
        """Create mock OI data."""
        dates = pd.date_range('2025-08-01', '2025-08-10', freq='D')

        data = []
        for date in dates:
            data.append({'date': date, 'market': 'WTI', 'open_interest': 500000})
            data.append({'date': date, 'market': 'BRENT', 'open_interest': 400000})

        return pd.DataFrame(data)

    def test_merge_data(self, mock_config, mock_cftc_data, mock_oi_data):
        """Test merging CFTC and OI data."""
        calc = RollPressureCalculator(mock_config)

        merged = calc.merge_data(mock_cftc_data, mock_oi_data)

        # Should have data for both markets
        assert len(merged) > 0

        # Should have all required columns
        assert 'date' in merged.columns
        assert 'market' in merged.columns
        assert 'spec_net_long' in merged.columns
        assert 'open_interest' in merged.columns

        # Should have both markets
        assert set(merged['market'].unique()) == {'WTI', 'BRENT'}

    def test_calculate_roll_pressure_formula(self, mock_config, mock_cftc_data, mock_oi_data):
        """Test roll pressure calculation formula."""
        calc = RollPressureCalculator(mock_config)

        # Merge data
        merged = calc.merge_data(mock_cftc_data, mock_oi_data)

        # Add mock days_to_expiry
        merged['days_to_expiry'] = 5

        # Calculate
        result = calc.calculate_roll_pressure(merged)

        # Check formula: (spec_net_long / open_interest) * days_to_expiry
        first_row = result.iloc[0]

        expected_ratio = first_row['spec_net_long'] / first_row['open_interest']
        expected_roll_pressure = expected_ratio * first_row['days_to_expiry']

        assert abs(first_row['positioning_ratio'] - expected_ratio) < 0.0001
        assert abs(first_row['roll_pressure'] - expected_roll_pressure) < 0.0001

    def test_roll_pressure_bounds(self, mock_config, mock_cftc_data, mock_oi_data):
        """Test roll pressure respects min/max bounds."""
        calc = RollPressureCalculator(mock_config)

        merged = calc.merge_data(mock_cftc_data, mock_oi_data)
        merged['days_to_expiry'] = 100  # Very high to trigger max bound

        result = calc.calculate_roll_pressure(merged)

        # Should be clipped to max_value (2.0)
        assert result['roll_pressure'].max() <= calc.max_value
        assert result['roll_pressure'].min() >= calc.min_value

    def test_add_alert_column_triggered(self, mock_config):
        """Test alert triggering when conditions met."""
        calc = RollPressureCalculator(mock_config)

        # Create data that should trigger alert
        data = pd.DataFrame({
            'date': [datetime(2025, 8, 1)],
            'market': ['WTI'],
            'roll_pressure': [0.70],  # > 0.65 threshold
            'days_to_expiry': [1]  # <= 2 threshold
        })

        result = calc.add_alert_column(data)

        assert 'ALERTE_48H' in result.columns
        assert result.iloc[0]['ALERTE_48H'] == True

    def test_add_alert_column_not_triggered_low_pressure(self, mock_config):
        """Test alert not triggered when pressure is low."""
        calc = RollPressureCalculator(mock_config)

        # Low roll pressure
        data = pd.DataFrame({
            'date': [datetime(2025, 8, 1)],
            'market': ['WTI'],
            'roll_pressure': [0.30],  # < 0.65 threshold
            'days_to_expiry': [1]
        })

        result = calc.add_alert_column(data)

        assert result.iloc[0]['ALERTE_48H'] == False

    def test_add_alert_column_not_triggered_far_expiry(self, mock_config):
        """Test alert not triggered when expiry is far."""
        calc = RollPressureCalculator(mock_config)

        # Far expiry
        data = pd.DataFrame({
            'date': [datetime(2025, 8, 1)],
            'market': ['WTI'],
            'roll_pressure': [0.70],  # > threshold
            'days_to_expiry': [10]  # > 2 days threshold
        })

        result = calc.add_alert_column(data)

        assert result.iloc[0]['ALERTE_48H'] == False

    def test_filter_low_open_interest(self, mock_config, mock_cftc_data, mock_oi_data):
        """Test filtering of low open interest values."""
        calc = RollPressureCalculator(mock_config)

        # Add low OI value
        mock_oi_data_with_low = mock_oi_data.copy()
        mock_oi_data_with_low.loc[0, 'open_interest'] = 500  # Below min of 1000

        merged = calc.merge_data(mock_cftc_data, mock_oi_data_with_low)
        merged['days_to_expiry'] = 5

        initial_count = len(merged)
        result = calc.calculate_roll_pressure(merged)

        # Should have filtered out the low OI row
        assert len(result) < initial_count

    def test_get_latest_alerts(self, mock_config):
        """Test getting latest alerts."""
        calc = RollPressureCalculator(mock_config)

        # Create data with multiple alerts
        data = pd.DataFrame({
            'date': [datetime(2025, 8, 1), datetime(2025, 8, 2), datetime(2025, 8, 3)],
            'market': ['WTI', 'WTI', 'WTI'],
            'roll_pressure': [0.70, 0.75, 0.80],
            'days_to_expiry': [3, 2, 1],
            'ALERTE_48H': [False, True, True]
        })

        latest = calc.get_latest_alerts(data)

        # Should only get the most recent alert
        assert len(latest) == 1
        assert latest.iloc[0]['date'] == datetime(2025, 8, 3)

    def test_get_latest_alerts_no_alerts(self, mock_config):
        """Test getting latest alerts when none exist."""
        calc = RollPressureCalculator(mock_config)

        data = pd.DataFrame({
            'date': [datetime(2025, 8, 1)],
            'market': ['WTI'],
            'ALERTE_48H': [False]
        })

        latest = calc.get_latest_alerts(data)

        assert len(latest) == 0

    def test_positioning_ratio_calculation(self, mock_config, mock_cftc_data, mock_oi_data):
        """Test positioning ratio calculation."""
        calc = RollPressureCalculator(mock_config)

        merged = calc.merge_data(mock_cftc_data, mock_oi_data)
        merged['days_to_expiry'] = 5

        result = calc.calculate_roll_pressure(merged)

        # Check positioning ratio
        assert 'positioning_ratio' in result.columns

        # Should be spec_net_long / open_interest
        for _, row in result.iterrows():
            expected_ratio = row['spec_net_long'] / row['open_interest']
            assert abs(row['positioning_ratio'] - expected_ratio) < 0.0001


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_dataframes(self):
        """Test handling of empty dataframes."""
        config = {'calculation': {'min_open_interest': 1000}}
        calc = RollPressureCalculator(config)

        empty_df = pd.DataFrame(columns=['date', 'market', 'spec_net_long'])
        empty_oi = pd.DataFrame(columns=['date', 'market', 'open_interest'])

        merged = calc.merge_data(empty_df, empty_oi)

        # Should return empty but valid DataFrame
        assert len(merged) == 0

    def test_zero_open_interest(self):
        """Test handling of zero open interest."""
        config = {'calculation': {'min_open_interest': 1000}}
        calc = RollPressureCalculator(config)

        data = pd.DataFrame({
            'date': [datetime(2025, 8, 1)],
            'market': ['WTI'],
            'spec_net_long': [100000],
            'open_interest': [0],
            'days_to_expiry': [5]
        })

        result = calc.calculate_roll_pressure(data)

        # Should filter out zero OI
        assert len(result) == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
