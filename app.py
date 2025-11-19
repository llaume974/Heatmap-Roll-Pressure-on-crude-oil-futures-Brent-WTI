"""
Interface Web Streamlit pour Roll Pressure Heatmap

Application web interactive pour visualiser et analyser la roll pressure
sur les futures pétroliers (WTI, Brent).

Usage:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime
import yaml
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.features.roll_pressure import RollPressureCalculator
from src.viz.heatmap import generate_heatmaps
from src.viz.excel_alert import export_to_excel
from src.utils.io import load_config, save_dataframe

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Roll Pressure Heatmap",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CUSTOM CSS
# ============================================================================

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .alert-card {
        background-color: #ffe6e6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ff4444;
    }
    .success-card {
        background-color: #e6ffe6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #44ff44;
    }
    .stButton>button {
        width: 100%;
        background-color: #1f77b4;
        color: white;
        border-radius: 0.5rem;
        padding: 0.5rem 1rem;
        font-size: 1.1rem;
        font-weight: bold;
    }
    .stButton>button:hover {
        background-color: #145a8c;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

if 'data' not in st.session_state:
    st.session_state.data = None

if 'config' not in st.session_state:
    st.session_state.config = load_config()

if 'last_run' not in st.session_state:
    st.session_state.last_run = None

if 'pipeline_status' not in st.session_state:
    st.session_state.pipeline_status = "Not run"

# ============================================================================
# SIDEBAR NAVIGATION
# ============================================================================

st.sidebar.title("Roll Pressure")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["Dashboard", "Heatmap", "Data Explorer", "Configuration"],
    label_visibility="collapsed"
)

st.sidebar.markdown("---")
st.sidebar.markdown("### À propos")
st.sidebar.info("""
**Roll Pressure Heatmap**

Système d'alerte automatique pour détecter les fenêtres de roll agressives sur futures pétrole (WTI, Brent).

**Nouvelle formule**:
```
RollPressure = PosScore × TimeWeight
```

[Documentation](README.md)
""")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def run_pipeline(lookback_days, markets):
    """Execute le pipeline roll pressure."""
    try:
        with st.spinner("🔄 Chargement des données CFTC..."):
            calculator = RollPressureCalculator(st.session_state.config)
            df = calculator.compute_roll_pressure(lookback_days=lookback_days)

        if df.empty:
            st.error("❌ Aucune donnée générée!")
            return None

        st.session_state.data = df
        st.session_state.last_run = datetime.now()
        st.session_state.pipeline_status = "Success"

        return df

    except Exception as e:
        st.error(f"❌ Erreur: {str(e)}")
        st.session_state.pipeline_status = "Error"
        return None


def create_interactive_heatmap(df, config):
    """Crée une heatmap interactive avec Plotly."""

    # Préparer les données pour la heatmap
    heatmap_config = config.get('heatmap', {})
    lookback_days = heatmap_config.get('lookback_days', 60)

    # Filtrer les derniers jours
    df_recent = df.sort_values('date').groupby('market').tail(lookback_days)

    # Pivot pour créer la matrice de heatmap
    pivot_data = df_recent.pivot(index='market', columns='date', values='roll_pressure')

    # Créer la heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=[d.strftime('%Y-%m-%d') for d in pivot_data.columns],
        y=pivot_data.index,
        colorscale='RdYlGn_r',
        zmid=0.5,
        zmin=0,
        zmax=1,
        hovertemplate='Market: %{y}<br>Date: %{x}<br>Roll Pressure: %{z:.3f}<extra></extra>',
        colorbar=dict(
            title="Roll Pressure",
            tickvals=[0, 0.35, 0.5, 0.65, 1.0],
            ticktext=['0.0', '0.35 (Low)', '0.5 (Med)', '0.65 (High)', '1.0']
        )
    ))

    fig.update_layout(
        title="Roll Pressure Heatmap (Derniers 60 jours)",
        xaxis_title="Date",
        yaxis_title="Market",
        height=400,
        xaxis=dict(tickangle=45)
    )

    return fig


def format_alert_row(row):
    """Formate une ligne d'alerte pour l'affichage."""
    return {
        'Market': row['market'],
        'Date': row['date'].strftime('%Y-%m-%d'),
        'Days to Expiry': row['days_to_expiry'],
        'PosScore': f"{row['pos_score']:.3f}",
        'TimeWeight': f"{row['time_weight']:.3f}",
        'Roll Pressure': f"{row['roll_pressure']:.3f}",
        'Alert': '🚨 YES'
    }


# ============================================================================
# PAGE 1: DASHBOARD
# ============================================================================

if page == "Dashboard":
    st.markdown('<div class="main-header">Dashboard Roll Pressure</div>', unsafe_allow_html=True)
    st.markdown("Système d'alerte automatique pour les futures pétroliers")
    st.markdown("---")

    # ========================================================================
    # Configuration Panel
    # ========================================================================

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Configuration")

        markets = st.multiselect(
            "Markets à analyser",
            options=['wti', 'brent'],
            default=['wti', 'brent']
        )

        lookback_days = st.slider(
            "Lookback days",
            min_value=30,
            max_value=180,
            value=90,
            step=10
        )

    with col2:
        st.subheader("Seuils")

        thresholds_config = st.session_state.config.get('thresholds', {})
        alert_config = st.session_state.config.get('alert', {})

        st.metric("Green Max", f"{thresholds_config.get('green_max', 0.35):.2f}", delta="Low pressure")
        st.metric("Orange Max", f"{thresholds_config.get('orange_max', 0.50):.2f}", delta="Medium pressure")
        st.metric("PosScore Threshold", f"{alert_config.get('pos_score_threshold', 0.80):.2f}", delta="80th percentile")

    # ========================================================================
    # Run Pipeline Button
    # ========================================================================

    st.markdown("---")

    if st.button("Run Pipeline", type="primary"):
        df = run_pipeline(lookback_days, markets)

        if df is not None:
            st.success("✅ Pipeline exécuté avec succès!")
            st.balloons()

    # ========================================================================
    # Results Display
    # ========================================================================

    if st.session_state.data is not None:
        df = st.session_state.data

        st.markdown("---")
        st.subheader("Résultats")

        # Metrics row
        col1, col2, col3, col4 = st.columns(4)

        alert_count = df['ALERTE_48H'].sum() if 'ALERTE_48H' in df.columns else 0

        with col1:
            st.metric("Total Records", len(df))

        with col2:
            st.metric("Markets", len(df['market'].unique()))

        with col3:
            date_range = f"{df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}"
            st.metric("Date Range", date_range)

        with col4:
            if alert_count > 0:
                st.metric("🚨 Active Alerts", alert_count, delta="Critical")
            else:
                st.metric("✅ Active Alerts", 0, delta="Normal")

        # Alerts section
        if alert_count > 0:
            st.markdown("---")
            st.markdown("### 🚨 ALERTES ACTIVES")

            alerts_df = df[df['ALERTE_48H'] == True].copy()
            alerts_df = alerts_df.sort_values('date', ascending=False)

            # Format alerts for display
            display_alerts = pd.DataFrame([format_alert_row(row) for _, row in alerts_df.iterrows()])

            st.dataframe(
                display_alerts,
                use_container_width=True,
                hide_index=True
            )

        # Last run info
        if st.session_state.last_run:
            st.info(f"Dernière exécution: {st.session_state.last_run.strftime('%Y-%m-%d %H:%M:%S')}")

    else:
        st.info("👆 Cliquez sur 'Run Pipeline' pour exécuter l'analyse")


# ============================================================================
# PAGE 2: HEATMAP
# ============================================================================

elif page == "Heatmap":
    st.markdown('<div class="main-header">Heatmap Visualisation</div>', unsafe_allow_html=True)
    st.markdown("---")

    if st.session_state.data is not None:
        df = st.session_state.data
        config = st.session_state.config

        # Display options
        col1, col2 = st.columns([3, 1])

        with col1:
            st.subheader("Heatmap Interactive")

        with col2:
            view_mode = st.selectbox(
                "Mode d'affichage",
                ["Interactive (Plotly)", "PNG", "HTML"]
            )

        # Display heatmap
        if view_mode == "Interactive (Plotly)":
            fig = create_interactive_heatmap(df, config)
            st.plotly_chart(fig, use_container_width=True)

        elif view_mode == "PNG":
            png_path = Path("output/heatmap_roll_pressure.png")
            if png_path.exists():
                st.image(str(png_path))
            else:
                st.warning("PNG heatmap not found. Run pipeline first.")

        elif view_mode == "HTML":
            html_path = Path("output/heatmap_roll_pressure.html")
            if html_path.exists():
                with open(html_path, 'r') as f:
                    html_content = f.read()
                st.components.v1.html(html_content, height=600, scrolling=True)
            else:
                st.warning("HTML heatmap not found. Run pipeline first.")

        # Légende
        st.markdown("---")
        st.subheader("Légende")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("### 🟢 Green")
            st.markdown("**Roll Pressure < 0.35**")
            st.markdown("Pression faible – Conditions normales")

        with col2:
            st.markdown("### 🟠 Orange")
            st.markdown("**0.35 ≤ Roll Pressure ≤ 0.50**")
            st.markdown("Pression modérée – Surveillance recommandée")

        with col3:
            st.markdown("### 🔴 Red")
            st.markdown("**Roll Pressure > 0.50**")
            st.markdown("Pression élevée – Risque de volatilité")

    else:
        st.warning("⚠️ Aucune donnée disponible. Veuillez exécuter le pipeline depuis le Dashboard.")


# ============================================================================
# PAGE 3: DATA EXPLORER
# ============================================================================

elif page == "Data Explorer":
    st.markdown('<div class="main-header">📈 Data Explorer</div>', unsafe_allow_html=True)
    st.markdown("---")

    if st.session_state.data is not None:
        df = st.session_state.data.copy()

        # Filters
        col1, col2, col3 = st.columns(3)

        with col1:
            market_filter = st.multiselect(
                "Filtrer par Market",
                options=df['market'].unique().tolist(),
                default=df['market'].unique().tolist()
            )

        with col2:
            date_range = st.date_input(
                "Filtrer par Date",
                value=(df['date'].min(), df['date'].max()),
                min_value=df['date'].min().date(),
                max_value=df['date'].max().date()
            )

        with col3:
            alert_filter = st.selectbox(
                "Filtrer par Alerte",
                ["Tous", "Alertes uniquement", "Pas d'alertes"]
            )

        # Apply filters
        filtered_df = df[df['market'].isin(market_filter)]

        if len(date_range) == 2:
            start_date, end_date = date_range
            filtered_df = filtered_df[
                (filtered_df['date'].dt.date >= start_date) &
                (filtered_df['date'].dt.date <= end_date)
            ]

        if alert_filter == "Alertes uniquement":
            filtered_df = filtered_df[filtered_df['ALERTE_48H'] == True]
        elif alert_filter == "Pas d'alertes":
            filtered_df = filtered_df[filtered_df['ALERTE_48H'] == False]

        # Display data
        st.subheader(f"Données ({len(filtered_df)} lignes)")

        # Format for display
        display_df = filtered_df.copy()
        display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
        display_df['ALERTE_48H'] = display_df['ALERTE_48H'].map({True: '🚨 YES', False: 'No'})

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )

        # Export buttons
        st.markdown("---")
        col1, col2 = st.columns(2)

        with col1:
            csv = filtered_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"roll_pressure_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

        with col2:
            excel_path = Path("output/roll_pressure_latest.xlsx")
            if excel_path.exists():
                with open(excel_path, 'rb') as f:
                    excel_data = f.read()
                st.download_button(
                    label="📥 Download Excel",
                    data=excel_data,
                    file_name="roll_pressure_latest.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    else:
        st.warning("⚠️ Aucune donnée disponible. Veuillez exécuter le pipeline depuis le Dashboard.")


# ============================================================================
# PAGE 4: CONFIGURATION
# ============================================================================

elif page == "⚙️ Configuration":
    st.markdown('<div class="main-header">⚙️ Configuration</div>', unsafe_allow_html=True)
    st.markdown("---")

    st.subheader("Configuration YAML")

    config_path = Path("config.yaml")

    if config_path.exists():
        with open(config_path, 'r') as f:
            config_content = f.read()

        # Display current config
        edited_config = st.text_area(
            "Éditer config.yaml",
            value=config_content,
            height=500
        )

        # Buttons
        col1, col2 = st.columns(2)

        with col1:
            if st.button("💾 Save Configuration"):
                try:
                    # Validate YAML
                    yaml.safe_load(edited_config)

                    # Save
                    with open(config_path, 'w') as f:
                        f.write(edited_config)

                    # Reload config in session state
                    st.session_state.config = load_config()

                    st.success("✅ Configuration sauvegardée!")

                except yaml.YAMLError as e:
                    st.error(f"❌ Erreur YAML: {str(e)}")

        with col2:
            if st.button("🔄 Reset to Defaults"):
                st.warning("Fonctionnalité à implémenter: restaurer config par défaut")

        # Documentation
        st.markdown("---")
        st.subheader("📖 Documentation Configuration")

        st.markdown("""
        **Sections principales**:

        - `markets`: Liste des markets à surveiller (`wti`, `brent`)
        - `thresholds`: Seuils de couleur pour la heatmap (0-1)
        - `alert`: Conditions pour déclencher les alertes
        - `calculation`: Paramètres du calcul roll pressure
        - `data_sources`: Configuration des APIs (CFTC, Yahoo Finance)

        **Exemple de modification**:
        ```yaml
        alert:
          days_threshold: 3              # Changer de 2 à 3 jours
          pos_score_threshold: 0.75      # Abaisser le seuil à 75e percentile
        ```
        """)

    else:
        st.error("❌ Fichier config.yaml introuvable!")


# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 1rem;'>
    Roll Pressure Heatmap v1.0 |
    Données: CFTC Socrata API |
    Powered by Streamlit
</div>
""", unsafe_allow_html=True)
