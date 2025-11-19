"""
Command-line interface for Roll Pressure pipeline.

Commands:
- run: Execute full pipeline (refresh data + calculations + outputs)
- refresh-data: Only refresh CFTC and OI data
- build-outputs: Generate visualizations and Excel from existing data
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from loguru import logger
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.io import load_config, save_dataframe, load_dataframe, ensure_dir
from src.utils.logging import setup_logger
from src.features.roll_pressure import RollPressureCalculator
from src.viz.heatmap import generate_heatmaps
from src.viz.excel_alert import export_to_excel


class RollPressureCLI:
    """CLI for Roll Pressure pipeline."""

    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize CLI.

        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = load_config(config_path)

        # Setup logging
        log_config = self.config.get('logging', {})
        setup_logger(level=log_config.get('level', 'INFO'))

        logger.info("=" * 70)
        logger.info("ROLL PRESSURE - BRENT/WTI HEATMAP & ALERT SYSTEM")
        logger.info("=" * 70)

    def cmd_run(self, days: int = 90, markets: Optional[list] = None, dry_run: bool = False):
        """
        Execute full pipeline.

        Args:
            days: Lookback days
            markets: Markets to process (default from config)
            dry_run: Simulation mode (no file saves)
        """
        logger.info("COMMAND: run (full pipeline)")
        logger.info(f"Parameters: days={days}, dry_run={dry_run}")

        start_time = datetime.now()

        try:
            # Override config if markets specified
            if markets:
                self.config['markets'] = markets
                logger.info(f"Markets override: {markets}")

            # Step 1: Calculate roll pressure
            logger.info("\n" + "=" * 70)
            logger.info("STEP 1: CALCULATING ROLL PRESSURE")
            logger.info("=" * 70)

            calculator = RollPressureCalculator(self.config)
            df = calculator.compute_roll_pressure(lookback_days=days)

            if df.empty:
                logger.error("❌ No data generated. Pipeline failed.")
                return 1

            # Save processed data
            if not dry_run:
                processed_path = Path(self.config.get('paths', {}).get('data_processed', 'data/processed'))
                ensure_dir(processed_path)

                output_csv = processed_path / f"roll_pressure_{datetime.now().strftime('%Y%m%d')}.csv"
                save_dataframe(df, str(output_csv), format='csv')
                logger.info(f"✓ Saved processed data to {output_csv}")

            # Step 2: Generate visualizations
            logger.info("\n" + "=" * 70)
            logger.info("STEP 2: GENERATING VISUALIZATIONS")
            logger.info("=" * 70)

            if not dry_run:
                heatmap_paths = generate_heatmaps(df, self.config)

                for format_type, path in heatmap_paths.items():
                    logger.info(f"✓ {format_type.upper()}: {path}")
            else:
                logger.info("(Dry run - skipping visualization)")

            # Step 3: Export to Excel
            logger.info("\n" + "=" * 70)
            logger.info("STEP 3: EXPORTING TO EXCEL")
            logger.info("=" * 70)

            if not dry_run:
                excel_path = export_to_excel(df, self.config)
                logger.info(f"✓ Excel: {excel_path}")
            else:
                logger.info("(Dry run - skipping Excel export)")

            # Summary
            logger.info("\n" + "=" * 70)
            logger.info("PIPELINE SUMMARY")
            logger.info("=" * 70)

            alert_count = df['ALERTE_48H'].sum() if 'ALERTE_48H' in df.columns else 0

            logger.info(f"Total records: {len(df)}")
            logger.info(f"Markets: {df['market'].unique().tolist()}")
            logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
            logger.info(f"Active alerts: {alert_count}")

            if alert_count > 0:
                logger.warning(f"\n🚨 {alert_count} ALERT(S) DETECTED!")

                latest_alerts = df[df['ALERTE_48H'] == True].sort_values('date', ascending=False).head(5)
                logger.warning("\nLatest alerts:")

                for _, alert in latest_alerts.iterrows():
                    logger.warning(
                        f"  • {alert['market']} on {alert['date'].strftime('%Y-%m-%d')}: "
                        f"RP={alert['roll_pressure']:.3f}, Days={alert['days_to_expiry']}"
                    )

            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"\n✓ Pipeline completed in {elapsed:.1f}s")

            return 0

        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            logger.exception(e)
            return 1

    def cmd_refresh_data(self, days: int = 90):
        """
        Refresh data sources only (CFTC + OI).

        Args:
            days: Lookback days
        """
        logger.info("COMMAND: refresh-data")

        try:
            calculator = RollPressureCalculator(self.config)

            logger.info("Refreshing CFTC and OI data...")
            cftc_df, oi_df = calculator.load_all_data(lookback_days=days)

            logger.info(f"✓ CFTC: {len(cftc_df)} records")
            logger.info(f"✓ OI: {len(oi_df)} records")
            logger.info("Data refresh complete.")

            return 0

        except Exception as e:
            logger.error(f"❌ Data refresh failed: {e}")
            return 1

    def cmd_build_outputs(self, input_file: Optional[str] = None):
        """
        Build outputs (heatmaps + Excel) from existing processed data.

        Args:
            input_file: Path to processed CSV (default: most recent)
        """
        logger.info("COMMAND: build-outputs")

        try:
            # Load processed data
            if input_file is None:
                # Find most recent processed file
                processed_dir = Path(self.config.get('paths', {}).get('data_processed', 'data/processed'))

                csv_files = list(processed_dir.glob('roll_pressure_*.csv'))

                if not csv_files:
                    logger.error("No processed data found. Run 'refresh-data' first.")
                    return 1

                input_file = str(max(csv_files, key=lambda p: p.stat().st_mtime))

            logger.info(f"Loading data from {input_file}")
            df = load_dataframe(input_file, format='csv')

            # Convert date column
            df['date'] = pd.to_datetime(df['date'])

            # Generate outputs
            logger.info("Generating visualizations...")
            heatmap_paths = generate_heatmaps(df, self.config)

            logger.info("Exporting to Excel...")
            excel_path = export_to_excel(df, self.config)

            logger.info("\n✓ Outputs generated:")
            for format_type, path in heatmap_paths.items():
                logger.info(f"  • {format_type.upper()}: {path}")
            logger.info(f"  • Excel: {excel_path}")

            return 0

        except Exception as e:
            logger.error(f"❌ Build outputs failed: {e}")
            logger.exception(e)
            return 1


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Roll Pressure Heatmap & Alert System for Brent/WTI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.cli run                      # Full pipeline
  python -m src.cli run --days 120           # 120 days lookback
  python -m src.cli run --markets wti        # WTI only
  python -m src.cli run --dry-run            # Simulation mode
  python -m src.cli refresh-data             # Refresh data sources
  python -m src.cli build-outputs            # Generate outputs only
        """
    )

    parser.add_argument(
        'command',
        choices=['run', 'refresh-data', 'build-outputs'],
        help='Command to execute'
    )

    parser.add_argument(
        '--days',
        type=int,
        default=90,
        help='Lookback days (default: 90)'
    )

    parser.add_argument(
        '--markets',
        type=str,
        help='Comma-separated markets (e.g., wti,brent)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulation mode (no file saves)'
    )

    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to config file (default: config.yaml)'
    )

    parser.add_argument(
        '--input',
        type=str,
        help='Input file for build-outputs command'
    )

    args = parser.parse_args()

    # Initialize CLI
    cli = RollPressureCLI(config_path=args.config)

    # Parse markets
    markets = args.markets.split(',') if args.markets else None

    # Execute command
    if args.command == 'run':
        exit_code = cli.cmd_run(days=args.days, markets=markets, dry_run=args.dry_run)
    elif args.command == 'refresh-data':
        exit_code = cli.cmd_refresh_data(days=args.days)
    elif args.command == 'build-outputs':
        exit_code = cli.cmd_build_outputs(input_file=args.input)
    else:
        parser.print_help()
        exit_code = 1

    sys.exit(exit_code)


if __name__ == '__main__':
    main()
