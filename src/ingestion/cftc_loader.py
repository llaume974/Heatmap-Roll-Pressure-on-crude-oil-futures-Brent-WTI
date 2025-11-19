"""
CFTC Commitments of Traders (COT) data loader.

Downloads and processes CFTC Disaggregated Futures reports to extract
Managed Money positions for WTI and Brent crude oil.
"""

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from loguru import logger
from sodapy import Socrata


class CFTCLoader:
    """Load and process CFTC COT Disaggregated reports."""

    # CFTC market identification (Socrata API names)
    MARKET_MAPPINGS = {
        'wti': {
            'market_name': 'WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE',
            'alt_market_name': 'CRUDE OIL, LIGHT SWEET-WTI',  # Alternative match
            'cftc_code': '067651',  # NYMEX WTI
            'cftc_market_code': 'CL'
        },
        'brent': {
            'market_name': 'BRENT LAST DAY - NEW YORK MERCANTILE EXCHANGE',
            'alt_market_name': 'BRENT LAST DAY',  # Partial match for other exchanges
            'cftc_code': '0B3',
            'cftc_market_code': 'B'
        }
    }

    # Column mappings for CFTC Disaggregated report (Socrata API)
    # Socrata API uses lowercase with underscores
    COLUMN_MAPPINGS = {
        'report_date': 'report_date_as_yyyy_mm_dd',
        'market_name': 'market_and_exchange_names',
        'cftc_contract_code': 'cftc_contract_market_code',
        'mm_long': 'm_money_positions_long_all',
        'mm_short': 'm_money_positions_short_all',
        'open_interest': 'open_interest_all'
    }

    def __init__(self, cache_dir: str = "data/raw", config: Optional[Dict] = None):
        """
        Initialize CFTC loader.

        Args:
            cache_dir: Directory for caching downloaded files
            config: Configuration dict with CFTC settings
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.config = config or {}

        # Socrata API configuration
        cftc_config = self.config.get('data_sources', {}).get('cftc', {})
        self.api_domain = cftc_config.get('api_domain', 'publicreporting.cftc.gov')
        self.dataset_id = cftc_config.get('dataset_id', '72hh-3qpy')

        # Initialize Socrata client (no authentication needed for public data)
        self.client = Socrata(self.api_domain, None)

    def get_cache_path(self, start_date: datetime, end_date: datetime) -> Path:
        """Get cache file path for a given date range."""
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        return self.cache_dir / f"cftc_disagg_{start_str}_{end_str}.csv"

    def fetch_cftc_data_api(self, start_date: datetime, end_date: datetime,
                           force_refresh: bool = False) -> Optional[pd.DataFrame]:
        """
        Fetch CFTC Disaggregated report from Socrata API for a date range.

        Args:
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            force_refresh: Force re-download even if cached

        Returns:
            DataFrame with CFTC data or None if failed
        """
        cache_path = self.get_cache_path(start_date, end_date)

        # Check cache
        if cache_path.exists() and not force_refresh:
            try:
                logger.info(f"Loading CFTC data from cache: {cache_path}")
                df = pd.read_csv(cache_path, low_memory=False)
                # Parse dates if they exist
                if self.COLUMN_MAPPINGS['report_date'] in df.columns:
                    df[self.COLUMN_MAPPINGS['report_date']] = pd.to_datetime(
                        df[self.COLUMN_MAPPINGS['report_date']]
                    )
                return df
            except Exception as e:
                logger.warning(f"Cache read failed: {e}. Re-fetching from API.")

        # Fetch from Socrata API
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        logger.info(f"Fetching CFTC data from Socrata API: {start_str} to {end_str}")

        try:
            # Build SoQL query to filter by date range
            # Limit set high to get all data (API default is 1000)
            where_clause = f"{self.COLUMN_MAPPINGS['report_date']} >= '{start_str}' AND {self.COLUMN_MAPPINGS['report_date']} <= '{end_str}'"

            results = self.client.get(
                self.dataset_id,
                where=where_clause,
                limit=50000  # High limit to ensure we get all data
            )

            if not results:
                logger.warning(f"No data returned from API for date range {start_str} to {end_str}")
                return None

            # Convert to DataFrame
            df = pd.DataFrame.from_records(results)

            # Parse date column
            if self.COLUMN_MAPPINGS['report_date'] in df.columns:
                df[self.COLUMN_MAPPINGS['report_date']] = pd.to_datetime(
                    df[self.COLUMN_MAPPINGS['report_date']]
                )

            # Cache it
            df.to_csv(cache_path, index=False)
            logger.info(f"✓ Fetched and cached {len(df)} rows to {cache_path}")

            return df

        except Exception as e:
            logger.error(f"Failed to fetch CFTC data from API: {e}")
            return None

    def extract_market_data(self, df: pd.DataFrame, market: str) -> pd.DataFrame:
        """
        Extract data for a specific market (WTI or Brent).

        Args:
            df: Full CFTC DataFrame
            market: 'wti' or 'brent'

        Returns:
            Filtered DataFrame for that market
        """
        market = market.lower()

        if market not in self.MARKET_MAPPINGS:
            raise ValueError(f"Unknown market: {market}. Use 'wti' or 'brent'")

        mapping = self.MARKET_MAPPINGS[market]
        market_name_col = self.COLUMN_MAPPINGS['market_name']

        # Try primary market name
        market_df = df[df[market_name_col].str.contains(mapping['market_name'], case=False, na=False)]

        # Fallback to alternative name if no results (naming inconsistencies across exchanges)
        if len(market_df) == 0 and 'alt_market_name' in mapping:
            market_df = df[df[market_name_col].str.contains(mapping['alt_market_name'], case=False, na=False)]

        if len(market_df) == 0:
            logger.warning(f"No data found for market {market}. Check market name mapping.")

        return market_df

    def calculate_spec_net_long(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Spec_Net_Long = Managed Money Long - Managed Money Short.

        Args:
            df: DataFrame with MM Long/Short columns

        Returns:
            DataFrame with spec_net_long column added
        """
        mm_long_col = self.COLUMN_MAPPINGS['mm_long']
        mm_short_col = self.COLUMN_MAPPINGS['mm_short']

        # Handle potential missing columns
        if mm_long_col not in df.columns:
            logger.error(f"Column not found: {mm_long_col}. Available: {df.columns.tolist()}")
            # Try alternative column names
            alt_long = [c for c in df.columns if 'money' in c.lower() and 'long' in c.lower()]
            if alt_long:
                mm_long_col = alt_long[0]
                logger.warning(f"Using alternative column: {mm_long_col}")
            else:
                raise KeyError(f"Cannot find Managed Money Long column")

        if mm_short_col not in df.columns:
            alt_short = [c for c in df.columns if 'money' in c.lower() and 'short' in c.lower()]
            if alt_short:
                mm_short_col = alt_short[0]
                logger.warning(f"Using alternative column: {mm_short_col}")
            else:
                raise KeyError(f"Cannot find Managed Money Short column")

        df = df.copy()

        # Convert to numeric (Socrata API returns strings)
        df[mm_long_col] = pd.to_numeric(df[mm_long_col], errors='coerce')
        df[mm_short_col] = pd.to_numeric(df[mm_short_col], errors='coerce')

        df['spec_net_long'] = df[mm_long_col] - df[mm_short_col]

        return df

    def normalize_cftc_data(self, df: pd.DataFrame, market: str) -> pd.DataFrame:
        """
        Normalize CFTC data to standard format.

        Args:
            df: Raw market DataFrame
            market: Market name

        Returns:
            Normalized DataFrame with columns: date, market, spec_net_long, open_interest
        """
        date_col = self.COLUMN_MAPPINGS['report_date']
        oi_col = self.COLUMN_MAPPINGS['open_interest']

        # Calculate spec_net_long
        df = self.calculate_spec_net_long(df)

        # Convert open_interest to numeric (Socrata API returns strings)
        df[oi_col] = pd.to_numeric(df[oi_col], errors='coerce')

        # Select and rename columns
        normalized = pd.DataFrame({
            'date': pd.to_datetime(df[date_col]),
            'market': market.upper(),
            'spec_net_long': df['spec_net_long'],
            'open_interest': df[oi_col]
        })

        # Sort by date
        normalized = normalized.sort_values('date').reset_index(drop=True)

        return normalized

    def load_cftc_data(self, markets: List[str] = ['wti', 'brent'],
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None,
                       force_refresh: bool = False) -> pd.DataFrame:
        """
        Load CFTC data for specified markets and date range.

        Args:
            markets: List of markets to load (e.g., ['wti', 'brent'])
            start_date: Start date (default: 2 years ago)
            end_date: End date (default: today)
            force_refresh: Force re-download

        Returns:
            Combined DataFrame with all markets (includes spec_net_long and open_interest)
        """
        if end_date is None:
            end_date = datetime.now()

        if start_date is None:
            start_date = end_date - timedelta(days=730)  # 2 years

        # Fetch data from API
        df_all = self.fetch_cftc_data_api(start_date, end_date, force_refresh)

        if df_all is None:
            logger.error("No CFTC data loaded from API!")
            return pd.DataFrame(columns=['date', 'market', 'spec_net_long', 'open_interest'])

        all_data = []

        for market in markets:
            try:
                market_df = self.extract_market_data(df_all, market)

                if len(market_df) == 0:
                    logger.warning(f"No data found for market {market}")
                    continue

                normalized = self.normalize_cftc_data(market_df, market)
                all_data.append(normalized)

            except Exception as e:
                logger.error(f"Error processing {market}: {e}")
                continue

        if not all_data:
            logger.error("No CFTC data loaded for any market!")
            return pd.DataFrame(columns=['date', 'market', 'spec_net_long', 'open_interest'])

        # Combine all data
        combined = pd.concat(all_data, ignore_index=True)

        # Filter date range (extra safety check)
        combined = combined[(combined['date'] >= start_date) & (combined['date'] <= end_date)]

        logger.info(f"✓ Loaded {len(combined)} CFTC records for {markets}")

        return combined

    def forward_fill_daily(self, df: pd.DataFrame, end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Forward-fill weekly CFTC data to daily frequency.

        Args:
            df: DataFrame with date, market, spec_net_long, open_interest columns
            end_date: End date for daily range (default: today)

        Returns:
            Daily DataFrame with forward-filled values
        """
        if end_date is None:
            end_date = datetime.now()

        if len(df) == 0:
            return df

        # Get date range
        start_date = df['date'].min()

        # Create daily date range
        daily_dates = pd.date_range(start=start_date, end=end_date, freq='D')

        # Forward fill for each market separately
        markets = df['market'].unique()
        daily_data = []

        for market in markets:
            market_df = df[df['market'] == market].copy()

            # Create daily template
            daily_template = pd.DataFrame({'date': daily_dates, 'market': market})

            # Merge and forward fill
            merged = daily_template.merge(market_df, on=['date', 'market'], how='left')
            merged['spec_net_long'] = merged['spec_net_long'].ffill()

            # Forward fill open_interest if it exists
            if 'open_interest' in merged.columns:
                merged['open_interest'] = merged['open_interest'].ffill()

            # Drop rows with no data (before first CFTC report)
            merged = merged.dropna(subset=['spec_net_long'])

            daily_data.append(merged)

        result = pd.concat(daily_data, ignore_index=True)
        result = result.sort_values(['market', 'date']).reset_index(drop=True)

        logger.info(f"✓ Forward-filled to {len(result)} daily records")

        return result


def load_cftc_data(markets: List[str] = ['wti', 'brent'],
                   lookback_days: int = 90,
                   config: Optional[Dict] = None) -> pd.DataFrame:
    """
    Convenience function to load CFTC data.

    Args:
        markets: List of markets
        lookback_days: Days to look back from today
        config: Configuration dict

    Returns:
        Daily DataFrame with spec_net_long data
    """
    loader = CFTCLoader(config=config)

    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days + 30)  # Add buffer for forward-fill

    df = loader.load_cftc_data(markets, start_date, end_date)

    # Forward fill to daily
    df_daily = loader.forward_fill_daily(df, end_date)

    return df_daily
