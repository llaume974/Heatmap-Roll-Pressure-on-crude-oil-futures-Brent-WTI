"""
Roll Pressure calculation and feature engineering.

Merges CFTC positioning, Open Interest, and expiry data to calculate
the Roll Pressure indicator: (Spec_Net_Long / OI) × DaysToExpiry
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
from loguru import logger

from ..ingestion.cftc_loader import load_cftc_data
from ..ingestion.oi_loader import load_oi_data
from ..ingestion.expiry_calendar import ExpiryCalendar
from ..utils.io import load_config


class RollPressureCalculator:
    """Calculate roll pressure indicator from multiple data sources."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize roll pressure calculator.

        Args:
            config: Configuration dictionary
        """
        if config is None:
            config = load_config()

        self.config = config

        # Calculation parameters
        calc_config = self.config.get('calculation', {})
        self.min_value = calc_config.get('min_value', 0.0)
        self.max_value = calc_config.get('max_value', 2.0)
        self.min_open_interest = calc_config.get('min_open_interest', 1000)

        # Markets to process
        self.markets = self.config.get('markets', ['wti', 'brent'])

        # Expiry calendar
        calendar_path = self.config.get('paths', {}).get('calendar', 'calendar/contracts.csv')
        self.calendar = ExpiryCalendar(calendar_path)

    def load_all_data(self, lookback_days: int = 90) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load all required data sources (CFTC with OI, fallback to Yahoo Finance if needed).

        Args:
            lookback_days: Days to look back

        Returns:
            Tuple of (cftc_df, oi_df) - oi_df may be empty if OI is in cftc_df
        """
        logger.info(f"Loading data for {lookback_days} days lookback...")

        # Load CFTC data (already forward-filled to daily, now includes open_interest)
        logger.info("Loading CFTC data (includes Open Interest)...")
        cftc_df = load_cftc_data(
            markets=self.markets,
            lookback_days=lookback_days,
            config=self.config
        )

        # Check if CFTC data already contains Open Interest
        if 'open_interest' in cftc_df.columns and not cftc_df['open_interest'].isna().all():
            logger.info("✓ Open Interest available from CFTC API")
            # Return empty OI dataframe since it's already in CFTC
            oi_df = pd.DataFrame(columns=['date', 'market', 'open_interest'])
        else:
            # Fallback to Yahoo Finance OI data
            logger.info("Loading Open Interest data from Yahoo Finance (fallback)...")
            oi_df = load_oi_data(
                markets=self.markets,
                lookback_days=lookback_days,
                config=self.config
            )

        logger.info(f"✓ Loaded CFTC: {len(cftc_df)} rows, OI: {len(oi_df)} rows")

        return cftc_df, oi_df

    def add_days_to_expiry(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add days_to_expiry column for each row.

        Args:
            df: DataFrame with 'date' and 'market' columns

        Returns:
            DataFrame with 'days_to_expiry' column added
        """
        logger.info("Calculating days to expiry...")

        df = df.copy()
        days_to_expiry_list = []

        for _, row in df.iterrows():
            date = row['date']
            market = row['market'].lower()

            # Convert to datetime if needed
            if isinstance(date, str):
                date = pd.to_datetime(date)

            # Get days to expiry
            days = self.calendar.days_to_expiry(market, date.to_pydatetime())

            days_to_expiry_list.append(days if days is not None else 0)

        df['days_to_expiry'] = days_to_expiry_list

        logger.info(f"✓ Added days_to_expiry (range: {df['days_to_expiry'].min()}-{df['days_to_expiry'].max()})")

        return df

    def merge_data(self, cftc_df: pd.DataFrame, oi_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge CFTC and OI data on date and market.
        If OI is already in cftc_df, just return cftc_df.

        Args:
            cftc_df: CFTC DataFrame (date, market, spec_net_long, potentially open_interest)
            oi_df: OI DataFrame (date, market, open_interest) - may be empty

        Returns:
            Merged DataFrame
        """
        logger.info("Merging CFTC and OI data...")

        cftc_df = cftc_df.copy()
        cftc_df['date'] = pd.to_datetime(cftc_df['date'])
        cftc_df['market'] = cftc_df['market'].str.upper()

        # If OI is already in CFTC data, no need to merge
        if 'open_interest' in cftc_df.columns and len(oi_df) == 0:
            logger.info(f"✓ Using Open Interest from CFTC: {len(cftc_df)} rows")
            return cftc_df

        # Otherwise, merge with external OI data
        oi_df = oi_df.copy()
        oi_df['date'] = pd.to_datetime(oi_df['date'])
        oi_df['market'] = oi_df['market'].str.upper()

        # Merge on date and market
        merged = pd.merge(
            cftc_df,
            oi_df,
            on=['date', 'market'],
            how='inner'  # Only keep rows with both CFTC and OI data
        )

        logger.info(f"✓ Merged data: {len(merged)} rows (from {len(cftc_df)} CFTC + {len(oi_df)} OI)")

        if len(merged) == 0:
            logger.warning("⚠️  No overlapping data found! Check date ranges and market names.")

        return merged

    def calculate_pos_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate PosScore as percentile rank of positioning_ratio.

        Uses a rolling lookback window to calculate the percentile rank
        of current positioning relative to historical values (0-1 scale).

        Args:
            df: DataFrame with positioning_ratio column

        Returns:
            DataFrame with pos_score column added
        """
        logger.info("Calculating PosScore (percentile rank)...")

        df = df.copy()
        calc_config = self.config.get('calculation', {})
        lookback = calc_config.get('lookback_percentile', 252)  # 1 trading year

        # Calculate percentile rank for each market separately
        def percentile_rank(series):
            """Calculate percentile rank within rolling window."""
            return series.rank(pct=True)

        # Group by market and calculate rolling percentile
        pos_scores = []

        for market in df['market'].unique():
            market_df = df[df['market'] == market].copy()
            market_df = market_df.sort_values('date')

            # Calculate rolling percentile rank
            # For each point, find its percentile within the lookback window
            pos_score_list = []

            for idx in range(len(market_df)):
                # Get lookback window (up to lookback days before current date)
                start_idx = max(0, idx - lookback + 1)
                window = market_df.iloc[start_idx:idx+1]['positioning_ratio']

                # Calculate percentile rank (0-1 scale)
                current_value = market_df.iloc[idx]['positioning_ratio']
                percentile = (window < current_value).sum() / len(window)
                pos_score_list.append(percentile)

            market_df['pos_score'] = pos_score_list
            pos_scores.append(market_df)

        result = pd.concat(pos_scores, ignore_index=True)
        result = result.sort_values(['market', 'date']).reset_index(drop=True)

        logger.info(f"✓ PosScore calculated (range: {result['pos_score'].min():.3f}-{result['pos_score'].max():.3f})")

        return result

    def calculate_time_weight(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate TimeWeight using inverse relationship with days to expiry.

        Formula: TimeWeight = (1 + α) / (d + α)
        where d = days_to_expiry (clipped to minimum 1) and α = time_weight_alpha

        This ensures TimeWeight approaches 1.0 as expiry approaches (high pressure)
        and decreases as expiry is further away (low pressure).

        Args:
            df: DataFrame with days_to_expiry column

        Returns:
            DataFrame with time_weight column added
        """
        logger.info("Calculating TimeWeight...")

        df = df.copy()
        calc_config = self.config.get('calculation', {})
        alpha = calc_config.get('time_weight_alpha', 1.0)

        # Clip days to minimum 1 to avoid division issues
        d = df['days_to_expiry'].clip(lower=1)

        # Calculate time weight: (1 + α) / (d + α)
        df['time_weight'] = (1 + alpha) / (d + alpha)

        logger.info(f"✓ TimeWeight calculated (range: {df['time_weight'].min():.3f}-{df['time_weight'].max():.3f})")

        return df

    def calculate_roll_pressure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate roll pressure indicator using new formula.

        New Formula: RollPressure = PosScore × TimeWeight
        where:
        - PosScore = Percentile rank of positioning_ratio (0-1 scale)
        - TimeWeight = (1+α)/(d+α) where d=days_to_expiry, α=time_weight_alpha

        This formula ensures roll pressure INCREASES as expiry approaches,
        reflecting market reality where roll pressure is highest when positions
        must be rolled immediately.

        Args:
            df: Merged DataFrame with spec_net_long, open_interest, days_to_expiry

        Returns:
            DataFrame with roll_pressure column added
        """
        logger.info("Calculating roll pressure...")

        df = df.copy()

        # Filter out rows with insufficient OI
        initial_count = len(df)
        df = df[df['open_interest'] >= self.min_open_interest].copy()

        if len(df) < initial_count:
            logger.warning(f"Filtered out {initial_count - len(df)} rows with OI < {self.min_open_interest}")

        # Calculate positioning ratio
        df['positioning_ratio'] = df['spec_net_long'] / df['open_interest']

        # Calculate PosScore (percentile rank normalization)
        df = self.calculate_pos_score(df)

        # Calculate TimeWeight (inverse of days to expiry)
        df = self.calculate_time_weight(df)

        # New formula: RollPressure = PosScore × TimeWeight
        df['roll_pressure'] = df['pos_score'] * df['time_weight']

        # Clip to [0, 1] range (normalized scale)
        df['roll_pressure'] = df['roll_pressure'].clip(lower=self.min_value, upper=self.max_value)

        # Log statistics
        logger.info(f"Roll Pressure stats: min={df['roll_pressure'].min():.3f}, "
                    f"max={df['roll_pressure'].max():.3f}, "
                    f"mean={df['roll_pressure'].mean():.3f}")

        return df

    def add_alert_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add ALERTE_48H column based on new thresholds.

        Alert triggered if:
        - pos_score >= pos_score_threshold (e.g., 80th percentile) AND
        - days_to_expiry <= days_threshold (e.g., 2 days)

        This logic captures the market reality: "la pression vient après le roll"
        - pressure comes when we're CLOSE to expiry (few days left)
        - AND positioning is at historical extremes (high percentile)

        Args:
            df: DataFrame with pos_score and days_to_expiry

        Returns:
            DataFrame with ALERTE_48H column
        """
        alert_config = self.config.get('alert', {})
        pos_score_threshold = alert_config.get('pos_score_threshold', 0.80)
        days_threshold = alert_config.get('days_threshold', 2)

        df = df.copy()

        # Check if required columns exist
        if 'pos_score' not in df.columns or 'days_to_expiry' not in df.columns:
            df['ALERTE_48H'] = False
            return df

        # New alert logic: high positioning percentile + close to expiry
        df['ALERTE_48H'] = (
            (df['pos_score'] >= pos_score_threshold) &
            (df['days_to_expiry'] <= days_threshold)
        )

        alert_count = df['ALERTE_48H'].sum()

        if alert_count > 0:
            logger.warning(f"🚨 {alert_count} ALERTS detected!")
        else:
            logger.info("✓ No alerts triggered")

        return df

    def compute_roll_pressure(self, lookback_days: int = 90) -> pd.DataFrame:
        """
        Complete roll pressure calculation pipeline.

        Args:
            lookback_days: Days to look back

        Returns:
            Final DataFrame with all features and roll pressure
        """
        logger.info("=" * 60)
        logger.info("ROLL PRESSURE CALCULATION PIPELINE")
        logger.info("=" * 60)

        # Step 1: Load data
        cftc_df, oi_df = self.load_all_data(lookback_days)

        # Check if we have CFTC data and either OI data or OI is already in CFTC
        has_oi = len(oi_df) > 0 or ('open_interest' in cftc_df.columns and not cftc_df['open_interest'].isna().all())

        if len(cftc_df) == 0 or not has_oi:
            logger.error("Insufficient data to compute roll pressure!")
            return pd.DataFrame()

        # Step 2: Merge
        merged = self.merge_data(cftc_df, oi_df)

        if len(merged) == 0:
            logger.error("No merged data available!")
            return pd.DataFrame()

        # Step 3: Add days to expiry
        with_expiry = self.add_days_to_expiry(merged)

        # Step 4: Calculate roll pressure
        with_roll_pressure = self.calculate_roll_pressure(with_expiry)

        # Step 5: Add alerts
        final = self.add_alert_column(with_roll_pressure)

        # Sort by market and date
        final = final.sort_values(['market', 'date']).reset_index(drop=True)

        # Select final columns (including new pos_score and time_weight)
        columns = [
            'date', 'market', 'spec_net_long', 'open_interest',
            'days_to_expiry', 'positioning_ratio', 'pos_score', 'time_weight',
            'roll_pressure', 'ALERTE_48H'
        ]

        final = final[columns]

        logger.info("=" * 60)
        logger.info(f"✓ PIPELINE COMPLETE: {len(final)} rows generated")
        logger.info("=" * 60)

        return final

    def get_latest_alerts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Get the latest alerts (most recent date with alert for each market).

        Args:
            df: Roll pressure DataFrame

        Returns:
            DataFrame with only current alerts
        """
        alerts = df[df['ALERTE_48H'] == True].copy()

        if len(alerts) == 0:
            return pd.DataFrame()

        # Get most recent alerts per market
        latest_alerts = alerts.loc[alerts.groupby('market')['date'].idxmax()]

        return latest_alerts


def compute_roll_pressure(lookback_days: int = 90, config_path: str = "config.yaml") -> pd.DataFrame:
    """
    Convenience function to compute roll pressure.

    Args:
        lookback_days: Days to look back
        config_path: Path to configuration file

    Returns:
        DataFrame with roll pressure calculations
    """
    config = load_config(config_path)
    calculator = RollPressureCalculator(config)

    return calculator.compute_roll_pressure(lookback_days)
