"""
Open Interest (OI) data loader.

Primary source: Yahoo Finance via yfinance
Fallback: Documented proxy strategies (last known value, volume-based estimation)
"""

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from loguru import logger
from pathlib import Path


class OILoader:
    """Load Open Interest data for crude oil futures."""

    # Yahoo Finance symbols
    SYMBOLS = {
        'wti': 'CL=F',    # NYMEX WTI front month
        'brent': 'BZ=F'   # ICE Brent front month
    }

    def __init__(self, cache_dir: str = "data/raw", config: Optional[Dict] = None):
        """
        Initialize OI loader.

        Args:
            cache_dir: Directory for caching OI data
            config: Configuration dict
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.config = config or {}

        # Fallback strategy: 'last_known', 'volume_proxy', or 'none'
        self.fallback_strategy = self.config.get('oi_fallback', 'last_known')

    def get_cache_path(self, market: str) -> Path:
        """Get cache file path for OI data."""
        return self.cache_dir / f"oi_{market.lower()}.json"

    def fetch_yfinance_oi(self, market: str, lookback_days: int = 90) -> pd.DataFrame:
        """
        Fetch Open Interest from Yahoo Finance.

        Args:
            market: 'wti' or 'brent'
            lookback_days: Days to fetch

        Returns:
            DataFrame with columns: date, market, open_interest
        """
        market = market.lower()

        if market not in self.SYMBOLS:
            raise ValueError(f"Unknown market: {market}")

        symbol = self.SYMBOLS[market]

        logger.info(f"Fetching OI for {market.upper()} ({symbol}) from Yahoo Finance...")

        try:
            # Download data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=lookback_days)

            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date, end=end_date)

            if hist.empty:
                logger.warning(f"No data returned from yfinance for {symbol}")
                return pd.DataFrame(columns=['date', 'market', 'open_interest'])

            # Check if openInterest column exists
            if 'Open Interest' not in hist.columns:
                logger.warning(f"Open Interest column not available for {symbol}")

                # Try alternative: use Volume as proxy (documented limitation)
                if self.fallback_strategy == 'volume_proxy' and 'Volume' in hist.columns:
                    logger.info(f"Using Volume as OI proxy for {market.upper()}")
                    hist['Open Interest'] = hist['Volume'] * 0.5  # Rough estimation
                else:
                    return pd.DataFrame(columns=['date', 'market', 'open_interest'])

            # Extract OI
            oi_data = hist[['Open Interest']].copy()
            oi_data = oi_data.reset_index()
            oi_data.columns = ['date', 'open_interest']

            # Add market column
            oi_data['market'] = market.upper()

            # Remove NaN values
            oi_data = oi_data.dropna(subset=['open_interest'])

            # Ensure date is datetime
            oi_data['date'] = pd.to_datetime(oi_data['date'])

            # Sort by date
            oi_data = oi_data.sort_values('date').reset_index(drop=True)

            logger.info(f"✓ Fetched {len(oi_data)} OI records for {market.upper()}")

            return oi_data[['date', 'market', 'open_interest']]

        except Exception as e:
            logger.error(f"Error fetching OI from yfinance for {market}: {e}")
            return pd.DataFrame(columns=['date', 'market', 'open_interest'])

    def apply_fallback_strategy(self, df: pd.DataFrame, full_date_range: pd.DatetimeIndex) -> pd.DataFrame:
        """
        Apply fallback strategy for missing OI values.

        Args:
            df: DataFrame with partial OI data
            full_date_range: Complete date range to fill

        Returns:
            DataFrame with filled values
        """
        if len(df) == 0:
            logger.warning("No OI data available for fallback")
            return df

        market = df['market'].iloc[0] if len(df) > 0 else 'UNKNOWN'

        # Create template with full date range
        template = pd.DataFrame({'date': full_date_range, 'market': market})

        # Merge with existing data
        merged = template.merge(df, on=['date', 'market'], how='left')

        # Apply fallback
        if self.fallback_strategy == 'last_known':
            # Forward-fill missing values
            merged['open_interest'] = merged['open_interest'].ffill()

            # Backward-fill for initial NaNs
            merged['open_interest'] = merged['open_interest'].bfill()

            logger.info(f"Applied 'last_known' fallback for {market}")

        elif self.fallback_strategy == 'volume_proxy':
            # Already handled in fetch_yfinance_oi
            pass

        elif self.fallback_strategy == 'none':
            # Leave NaNs as is
            pass

        # Drop remaining NaNs
        result = merged.dropna(subset=['open_interest'])

        return result

    def load_oi_data(self, markets: List[str] = ['wti', 'brent'],
                     lookback_days: int = 90,
                     force_refresh: bool = False) -> pd.DataFrame:
        """
        Load OI data for specified markets.

        Args:
            markets: List of markets
            lookback_days: Days to load
            force_refresh: Force refresh from source

        Returns:
            Combined DataFrame with OI data
        """
        all_data = []

        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        for market in markets:
            cache_path = self.get_cache_path(market)

            # Try loading from cache
            if cache_path.exists() and not force_refresh:
                try:
                    logger.info(f"Loading OI from cache: {cache_path}")
                    cached = pd.read_json(cache_path)
                    cached['date'] = pd.to_datetime(cached['date'])

                    # Check if cache is recent enough
                    if cached['date'].max() >= (end_date - timedelta(days=2)):
                        # Filter to requested range
                        filtered = cached[(cached['date'] >= start_date) & (cached['date'] <= end_date)]
                        all_data.append(filtered)
                        logger.info(f"✓ Using cached OI for {market.upper()}")
                        continue
                except Exception as e:
                    logger.warning(f"Cache read failed for {market}: {e}")

            # Fetch from yfinance
            oi_df = self.fetch_yfinance_oi(market, lookback_days + 30)  # Buffer for fallback

            if len(oi_df) > 0:
                # Apply fallback for missing values
                oi_df = self.apply_fallback_strategy(oi_df, date_range)

                # Cache result
                try:
                    oi_df.to_json(cache_path, orient='records', date_format='iso', indent=2)
                    logger.info(f"Cached OI data to {cache_path}")
                except Exception as e:
                    logger.warning(f"Failed to cache OI for {market}: {e}")

                all_data.append(oi_df)
            else:
                logger.warning(f"No OI data available for {market}")

        if not all_data:
            logger.error("No OI data loaded for any market!")
            return pd.DataFrame(columns=['date', 'market', 'open_interest'])

        # Combine all markets
        combined = pd.concat(all_data, ignore_index=True)

        # Filter to date range
        combined = combined[(combined['date'] >= start_date) & (combined['date'] <= end_date)]

        # Sort
        combined = combined.sort_values(['market', 'date']).reset_index(drop=True)

        logger.info(f"✓ Loaded {len(combined)} OI records across {len(markets)} markets")

        return combined

    def validate_oi_data(self, df: pd.DataFrame, min_oi: int = 1000) -> pd.DataFrame:
        """
        Validate OI data and filter out unrealistic values.

        Args:
            df: OI DataFrame
            min_oi: Minimum acceptable OI value

        Returns:
            Validated DataFrame
        """
        initial_count = len(df)

        # Remove very low OI (likely errors)
        df = df[df['open_interest'] >= min_oi].copy()

        # Remove negative values
        df = df[df['open_interest'] > 0].copy()

        removed = initial_count - len(df)

        if removed > 0:
            logger.warning(f"Removed {removed} OI records with invalid values (< {min_oi} or <= 0)")

        return df


def load_oi_data(markets: List[str] = ['wti', 'brent'],
                 lookback_days: int = 90,
                 config: Optional[Dict] = None) -> pd.DataFrame:
    """
    Convenience function to load OI data.

    Args:
        markets: List of markets
        lookback_days: Days to look back
        config: Configuration dict

    Returns:
        DataFrame with OI data
    """
    loader = OILoader(config=config)
    df = loader.load_oi_data(markets, lookback_days)
    df = loader.validate_oi_data(df)

    return df
