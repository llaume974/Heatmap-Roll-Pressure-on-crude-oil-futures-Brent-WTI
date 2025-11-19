"""Pytest configuration and shared fixtures."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    return {
        'markets': ['wti', 'brent'],
        'thresholds': {
            'green_max': 0.40,
            'orange_max': 0.65
        },
        'alert': {
            'red_threshold': 0.65,
            'days_threshold': 2
        },
        'calculation': {
            'min_value': 0.0,
            'max_value': 2.0,
            'min_open_interest': 1000
        },
        'heatmap': {
            'lookback_days': 60,
            'figsize_width': 14,
            'figsize_height': 6
        },
        'paths': {
            'calendar': 'calendar/contracts.csv',
            'data_raw': 'data/raw',
            'data_processed': 'data/processed'
        },
        'output_files': {
            'excel': 'output/roll_pressure_latest.xlsx',
            'heatmap_png': 'output/heatmap_roll_pressure.png',
            'heatmap_html': 'output/heatmap_roll_pressure.html'
        }
    }


@pytest.fixture
def mock_cftc_data():
    """Generate mock CFTC data with typical values."""
    dates = pd.date_range('2025-08-01', '2025-08-20', freq='D')

    data = []
    for date in dates:
        # WTI: Higher spec net long
        data.append({
            'date': date,
            'market': 'WTI',
            'spec_net_long': 150000 + (date.day * 500)  # Increasing trend
        })

        # Brent: Lower spec net long
        data.append({
            'date': date,
            'market': 'BRENT',
            'spec_net_long': 100000 + (date.day * 300)
        })

    return pd.DataFrame(data)


@pytest.fixture
def mock_oi_data():
    """Generate mock Open Interest data."""
    dates = pd.date_range('2025-08-01', '2025-08-20', freq='D')

    data = []
    for date in dates:
        # WTI: Stable OI
        data.append({
            'date': date,
            'market': 'WTI',
            'open_interest': 500000
        })

        # Brent: Slightly declining OI (increases roll pressure)
        data.append({
            'date': date,
            'market': 'BRENT',
            'open_interest': 400000 - (date.day * 1000)
        })

    return pd.DataFrame(data)


@pytest.fixture
def mock_roll_pressure_data():
    """Generate complete roll pressure dataset with alerts."""
    dates = pd.date_range('2025-08-01', '2025-08-20', freq='D')

    data = []
    for i, date in enumerate(dates):
        # Calculate days to expiry (decreasing)
        days_to_expiry = 20 - i

        # WTI: Moderate pressure, no alert
        wti_spec_net = 150000
        wti_oi = 500000
        wti_ratio = wti_spec_net / wti_oi
        wti_rp = wti_ratio * days_to_expiry

        data.append({
            'date': date,
            'market': 'WTI',
            'spec_net_long': wti_spec_net,
            'open_interest': wti_oi,
            'days_to_expiry': days_to_expiry,
            'positioning_ratio': wti_ratio,
            'roll_pressure': min(wti_rp, 2.0),
            'ALERTE_48H': False  # Never reaches alert condition
        })

        # Brent: High pressure, triggers alert near expiry
        brent_spec_net = 250000  # High positioning
        brent_oi = 350000  # Lower OI
        brent_ratio = brent_spec_net / brent_oi
        brent_rp = brent_ratio * days_to_expiry

        # Alert triggered when days <= 2 and RP > 0.65
        is_alert = (days_to_expiry <= 2) and (brent_rp > 0.65)

        data.append({
            'date': date,
            'market': 'BRENT',
            'spec_net_long': brent_spec_net,
            'open_interest': brent_oi,
            'days_to_expiry': days_to_expiry,
            'positioning_ratio': brent_ratio,
            'roll_pressure': min(brent_rp, 2.0),
            'ALERTE_48H': is_alert
        })

    return pd.DataFrame(data)


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create temporary data directory structure."""
    data_dir = tmp_path / "data"
    (data_dir / "raw").mkdir(parents=True)
    (data_dir / "processed").mkdir(parents=True)
    return data_dir
