"""
Heatmap visualization for roll pressure indicator.

Generates PNG and HTML heatmaps showing roll pressure over time
for WTI and Brent markets.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.colors import LinearSegmentedColormap, BoundaryNorm
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict
from loguru import logger

try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    logger.warning("Plotly not available. HTML heatmap will not be generated.")


class HeatmapGenerator:
    """Generate heatmap visualizations for roll pressure."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize heatmap generator.

        Args:
            config: Configuration dictionary
        """
        self.config = config or {}

        # Heatmap settings
        heatmap_config = self.config.get('heatmap', {})
        self.lookback_days = heatmap_config.get('lookback_days', 60)
        self.figsize = (
            heatmap_config.get('figsize_width', 14),
            heatmap_config.get('figsize_height', 6)
        )

        # Thresholds
        thresholds_config = self.config.get('thresholds', {})
        self.green_max = thresholds_config.get('green_max', 0.40)
        self.orange_max = thresholds_config.get('orange_max', 0.65)

        # Output paths
        output_config = self.config.get('output_files', {})
        self.png_path = output_config.get('heatmap_png', 'output/heatmap_roll_pressure.png')
        self.html_path = output_config.get('heatmap_html', 'output/heatmap_roll_pressure.html')

    def prepare_heatmap_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare data for heatmap (pivot to matrix format).

        Args:
            df: Roll pressure DataFrame with date, market, roll_pressure columns

        Returns:
            Pivoted DataFrame (dates × markets)
        """
        # Filter to lookback period
        if 'date' in df.columns:
            end_date = df['date'].max()
            start_date = end_date - timedelta(days=self.lookback_days)

            df = df[df['date'] >= start_date].copy()

        # Pivot: rows = markets, columns = dates, values = roll_pressure
        pivot = df.pivot_table(
            index='market',
            columns='date',
            values='roll_pressure',
            aggfunc='first'  # In case of duplicates
        )

        logger.info(f"Heatmap data prepared: {pivot.shape[0]} markets × {pivot.shape[1]} days")

        return pivot

    def create_custom_colormap(self):
        """
        Create custom colormap: Green → Yellow → Orange → Red

        Based on thresholds:
        - < green_max: Green
        - green_max to orange_max: Yellow/Orange
        - > orange_max: Red
        """
        # Define colors
        colors = ['#2ecc71', '#f1c40f', '#e67e22', '#e74c3c']  # Green, Yellow, Orange, Red

        # Create colormap
        n_bins = 100
        cmap = LinearSegmentedColormap.from_list('roll_pressure', colors, N=n_bins)

        # Create boundaries for discrete colors
        boundaries = [0, self.green_max, self.orange_max, 2.0]
        norm = BoundaryNorm(boundaries, cmap.N, clip=True)

        return cmap, norm

    def generate_png_heatmap(self, df: pd.DataFrame, save_path: Optional[str] = None) -> str:
        """
        Generate PNG heatmap using matplotlib.

        Args:
            df: Roll pressure DataFrame
            save_path: Custom save path (optional)

        Returns:
            Path to saved PNG file
        """
        logger.info("Generating PNG heatmap...")

        if save_path is None:
            save_path = self.png_path

        # Prepare data
        pivot = self.prepare_heatmap_data(df)

        if pivot.empty:
            logger.error("No data to visualize!")
            return ""

        # Create figure
        fig, ax = plt.subplots(figsize=self.figsize)

        # Create colormap
        cmap, norm = self.create_custom_colormap()

        # Plot heatmap
        im = ax.imshow(
            pivot.values,
            cmap=cmap,
            norm=norm,
            aspect='auto',
            interpolation='nearest'
        )

        # Set ticks
        ax.set_yticks(np.arange(len(pivot.index)))
        ax.set_yticklabels(pivot.index)

        # Format x-axis (dates)
        # Show every Nth date to avoid crowding
        n_dates = len(pivot.columns)
        step = max(1, n_dates // 15)  # Show ~15 date labels

        x_ticks_positions = np.arange(0, n_dates, step)
        x_ticks_labels = [pivot.columns[i].strftime('%Y-%m-%d') for i in x_ticks_positions]

        ax.set_xticks(x_ticks_positions)
        ax.set_xticklabels(x_ticks_labels, rotation=45, ha='right')

        # Labels
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Market', fontsize=12)
        ax.set_title('Roll Pressure Heatmap - Brent/WTI', fontsize=14, fontweight='bold')

        # Colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Roll Pressure', fontsize=11)

        # Add threshold lines to colorbar
        cbar.ax.axhline(y=self.green_max, color='black', linewidth=0.5, linestyle='--')
        cbar.ax.axhline(y=self.orange_max, color='black', linewidth=0.5, linestyle='--')

        # Add legend
        legend_text = (
            f"🟢 Low: < {self.green_max}\n"
            f"🟠 Medium: {self.green_max}-{self.orange_max}\n"
            f"🔴 High: > {self.orange_max}"
        )

        ax.text(
            1.15, 0.5, legend_text,
            transform=ax.transAxes,
            fontsize=10,
            verticalalignment='center',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3)
        )

        # Tight layout
        plt.tight_layout()

        # Save
        output_path = Path(save_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ PNG heatmap saved to {save_path}")

        return str(save_path)

    def generate_html_heatmap(self, df: pd.DataFrame, save_path: Optional[str] = None) -> str:
        """
        Generate interactive HTML heatmap using Plotly.

        Args:
            df: Roll pressure DataFrame
            save_path: Custom save path (optional)

        Returns:
            Path to saved HTML file
        """
        if not PLOTLY_AVAILABLE:
            logger.warning("Plotly not installed. Skipping HTML heatmap.")
            return ""

        logger.info("Generating HTML heatmap...")

        if save_path is None:
            save_path = self.html_path

        # Prepare data
        pivot = self.prepare_heatmap_data(df)

        if pivot.empty:
            logger.error("No data to visualize!")
            return ""

        # Create custom colorscale
        colorscale = [
            [0.0, '#2ecc71'],        # Green
            [self.green_max / 2.0, '#2ecc71'],
            [self.green_max / 2.0, '#f1c40f'],  # Yellow
            [self.orange_max / 2.0, '#e67e22'],  # Orange
            [self.orange_max / 2.0, '#e74c3c'],  # Red
            [1.0, '#c0392b']         # Dark red
        ]

        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=[d.strftime('%Y-%m-%d') for d in pivot.columns],
            y=pivot.index.tolist(),
            colorscale=colorscale,
            zmid=(self.green_max + self.orange_max) / 2,
            zmin=0,
            zmax=2.0,
            colorbar=dict(
                title="Roll Pressure",
                tickmode='array',
                tickvals=[0, self.green_max, self.orange_max, 2.0],
                ticktext=['0', f'{self.green_max}', f'{self.orange_max}', '2.0']
            ),
            hovertemplate='Date: %{x}<br>Market: %{y}<br>Roll Pressure: %{z:.3f}<extra></extra>'
        ))

        # Update layout
        fig.update_layout(
            title={
                'text': 'Roll Pressure Heatmap - Brent/WTI',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'weight': 'bold'}
            },
            xaxis={'title': 'Date', 'tickangle': 45},
            yaxis={'title': 'Market'},
            width=1200,
            height=400,
            template='plotly_white'
        )

        # Save
        output_path = Path(save_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.write_html(save_path)

        logger.info(f"✓ HTML heatmap saved to {save_path}")

        return str(save_path)

    def generate_all_heatmaps(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Generate both PNG and HTML heatmaps.

        Args:
            df: Roll pressure DataFrame

        Returns:
            Dict with paths to generated files
        """
        logger.info("Generating all heatmaps...")

        results = {}

        # PNG
        png_path = self.generate_png_heatmap(df)
        if png_path:
            results['png'] = png_path

        # HTML
        html_path = self.generate_html_heatmap(df)
        if html_path:
            results['html'] = html_path

        logger.info(f"✓ Generated {len(results)} heatmap(s)")

        return results


def generate_heatmaps(df: pd.DataFrame, config: Optional[Dict] = None) -> Dict[str, str]:
    """
    Convenience function to generate heatmaps.

    Args:
        df: Roll pressure DataFrame
        config: Configuration dict

    Returns:
        Dict with paths to generated files
    """
    generator = HeatmapGenerator(config)
    return generator.generate_all_heatmaps(df)
