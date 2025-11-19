#!/bin/bash
# Script de lancement de l'interface web Streamlit
# Usage: ./run_web.sh

echo "🔥 Lancement de Roll Pressure Heatmap Web Interface..."
echo ""

streamlit run app.py --server.port 8501 --server.address localhost

