# Heatmap Roll Pressure on crude oil futures Brent/WTI

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

**Système d'alerte automatique pour détecter les fenêtres de roll agressives sur futures pétrole**

> *"La pression vient après le roll"* – identifier les périodes à haut risque de volatilité lorsque les positions spéculatives extrêmes doivent être roulées dans les 48h précédant l'expiration.

* * *

## 1. Contexte & Problématique

### Le défi du rollover des futures

Les traders de futures pétroliers (Brent, WTI) font face à un défi critique lors des périodes de **rollover** – la transition mensuelle du contrat front-month vers le contrat suivant. Cette fenêtre de 2-3 jours avant expiration peut générer:

- **Volatilité explosive** si les spéculateurs (Managed Money) détiennent des positions extrêmes
- **Slippage significatif** lors du déroulement forcé des positions ($1-3/bbl)
- **Risque de timing** pour les hedges et arbitrages

### Le problème de l'ancienne approche

Les méthodes traditionnelles de calcul de "roll pressure" souffrent d'un **paradoxe fondamental**:

```
Ancienne formule: RollPressure = (SpecNetLong / OI) × DaysToExpiry
```

**Problème**: Cette formule génère une pression MAXIMALE lorsqu'on est LOIN de l'expiration (30 jours) et MINIMALE lorsqu'on est PROCHE (2 jours) – l'inverse de la réalité du marché!

### La solution

Ce projet implémente une nouvelle formule qui respecte la réalité du marché: **la pression est maximale quand on approche de l'expiration**.

* * *

## 2. Approche Technique

Le système suit un pipeline en **5 étapes** pour générer des alertes en temps quasi-réel:

1. **Data Ingestion** → Téléchargement automatique via **CFTC Socrata API** (COT Disaggregated) + Open Interest
2. **Feature Engineering** → Calcul du **PosScore** (percentile rank) et **TimeWeight** (inverse des jours restants)
3. **Roll Pressure Calculation** → Nouvelle formule: `RollPressure = PosScore × TimeWeight`
4. **Alert Detection** → Déclenchement si PosScore ≥ 80% ET jours ≤ 2
5. **Visualization** → Heatmap interactive + Excel avec mise en forme conditionnelle

* * *

## 3. La Nouvelle Formule

### Formulation mathématique

```
RollPressure = PosScore × TimeWeight
```

Où:
- **PosScore** = Percentile rank du ratio de positionnement sur 252 jours (1 an de trading)
  ```
  PosScore = Percentile(SpecNetLong / OpenInterest)
  ```

- **TimeWeight** = Amplification temporelle qui augmente à l'approche de l'expiration
  ```
  TimeWeight = (1 + α) / (d + α)
  ```
  - `d` = jours restants avant expiration (minimum 1)
  - `α` = paramètre de lissage (défaut: 1.0)

- **Spec_Net_Long** = Managed Money Long - Managed Money Short (CFTC COT)
- **Open_Interest** = Open Interest du contrat front-month (CFTC)

### Pourquoi cette formule?

| Composant | Rôle | Comportement |
|-----------|------|--------------|
| **PosScore** | Mesure si le positionnement est extrême historiquement | 0 = minimum historique, 1 = maximum historique |
| **TimeWeight** | Amplifie l'urgence à l'approche de l'expiration | 0.06 à 30 jours → 1.0 à 1 jour |
| **RollPressure** | Produit final normalisé [0, 1] | Capture la pression de roll réelle |

**Exemple concret**:
- **Scenario A**: PosScore = 0.90 (90e percentile), 30 jours → RP = 0.90 × 0.06 = 0.05 (faible)
- **Scenario B**: PosScore = 0.90 (90e percentile), 1 jour → RP = 0.90 × 1.0 = 0.90 (critique!)

✅ **La pression est maximale quand elle doit l'être: proche de l'expiration + positionnement extrême.**

* * *

## 4. Structure du Projet

```
heatmap/
├── README.md                     # Documentation (ce fichier)
├── config.yaml                   # Configuration (seuils, marchés, API)
├── requirements.txt              # Dépendances Python
├── Makefile                      # Commandes utiles
├── pytest.ini                    # Configuration tests
├── LICENSE                       # MIT License
├── app.py                        # Interface web Streamlit
├── run_web.sh                    # Script lancement web
│
├── src/                          # Code source
│   ├── cli.py                    # CLI (python -m src.cli run)
│   ├── ingestion/                # Récupération données
│   │   ├── cftc_loader.py        # CFTC Socrata API (COT)
│   │   ├── oi_loader.py          # Open Interest
│   │   └── expiry_calendar.py    # Calendrier expirations
│   ├── features/                 # Feature engineering
│   │   └── roll_pressure.py      # Calcul roll pressure
│   ├── viz/                      # Visualisations
│   │   ├── heatmap.py            # Heatmap PNG/HTML
│   │   └── excel_alert.py        # Export Excel
│   └── utils/                    # Utilitaires
│       ├── io.py                 # I/O helpers
│       ├── dates.py              # Gestion dates
│       └── logging.py            # Logging
│
├── tests/                        # Tests unitaires (63 tests, 100% passing)
│   ├── test_cftc_loader.py
│   ├── test_oi_loader.py
│   ├── test_expiry_calendar.py
│   ├── test_roll_pressure.py
│   └── test_integration.py
│
├── data/                         # Données (gitignored)
│   ├── raw/                      # Cache CFTC (CSV)
│   └── processed/                # Données traitées
│
├── calendar/                     # Données de référence
│   └── contracts.csv             # Calendrier 12-18 mois (WTI, Brent)
│
└── output/                       # Résultats (gitignored)
    ├── roll_pressure_latest.xlsx # Excel avec heatmap + data + alertes
    ├── heatmap_roll_pressure.png # Heatmap visuelle
    └── heatmap_roll_pressure.html# Heatmap interactive
```

* * *

## 5. Installation

### Prérequis
- Python 3.8 ou supérieur
- pip (gestionnaire de paquets Python)

### Installation des dépendances

```bash
# Cloner le projet
git clone <repository-url>
cd heatmap

# Installer les dépendances
pip install -r requirements.txt

# Ou utiliser le Makefile
make install
```

**Dépendances principales**:
- `pandas`, `numpy` – Manipulation de données
- `sodapy` – Client API Socrata (CFTC)
- `yfinance` – Fallback Open Interest (si CFTC indisponible)
- `matplotlib`, `plotly` – Visualisations
- `openpyxl` – Export Excel
- `streamlit` – Interface web
- `loguru` – Logging
- `pytest` – Tests

* * *

## 6. Configuration

Le fichier `config.yaml` contrôle tous les paramètres du système:

### Markets à surveiller
```yaml
markets:
  - wti     # NYMEX Crude Oil (CL)
  - brent   # ICE Brent (B)
```

### Seuils de la heatmap (normalisés 0-1)
```yaml
thresholds:
  green_max: 0.35      # 🟢 Pression faible
  orange_max: 0.50     # 🟠 Pression modérée
  # > 0.50             # 🔴 Pression élevée
```

### Conditions d'alerte
```yaml
alert:
  days_threshold: 2              # Alerte si ≤ 2 jours avant expiration
  pos_score_threshold: 0.80      # ET si PosScore ≥ 80e percentile
```

### Paramètres de calcul
```yaml
calculation:
  min_value: 0.0                 # Floor RollPressure
  max_value: 1.0                 # Ceiling RollPressure
  min_open_interest: 1000        # OI minimum valide
  time_weight_alpha: 1.0         # Alpha pour TimeWeight
  lookback_percentile: 252       # Fenêtre percentile (1 an trading)
```

### API CFTC
```yaml
data_sources:
  cftc:
    api_domain: 'publicreporting.cftc.gov'
    dataset_id: '72hh-3qpy'      # COT Disaggregated Futures Only
```

* * *

## 7. Utilisation

### 7.1 Mode CLI (Ligne de commande)

#### Pipeline complet (recommandé)
```bash
# Exécuter tout: ingestion + calcul + visualisations + Excel
python -m src.cli run

# Avec paramètres personnalisés
python -m src.cli run --days 120 --markets wti,brent

# Mode simulation (pas de sauvegarde)
python -m src.cli run --dry-run
```

#### Commandes spécifiques
```bash
# Rafraîchir uniquement les données CFTC
python -m src.cli refresh-data --days 90

# Régénérer visualisations depuis données existantes
python -m src.cli build-outputs
```

#### Makefile (shortcuts)
```bash
make install       # Installer dépendances
make run           # Pipeline complet
make test          # Lancer tests (63 tests, pytest)
make clean         # Nettoyer cache Python
make help          # Aide
```

### 7.2 Mode Web (Interface Streamlit)

#### Lancer l'interface web
```bash
# Méthode 1: Script direct
./run_web.sh

# Méthode 2: Commande Streamlit
streamlit run app.py

# Méthode 3: Port personnalisé
streamlit run app.py --server.port 8502
```

L'interface s'ouvre automatiquement sur **http://localhost:8501**

#### Fonctionnalités de l'interface web

**Page 1: Dashboard** 🎯
- Configuration: Markets, Lookback days, Thresholds
- Bouton "Run Pipeline" pour exécuter le calcul
- Métriques clés: Total records, Date range, Active alerts
- Section alertes (tableau rouge avec 🚨)

**Page 2: Heatmap** 🔥
- Visualisation interactive (Plotly)
- Affichage PNG/HTML
- Légende couleur: 🟢 Green (0-0.35), 🟠 Orange (0.35-0.50), 🔴 Red (>0.50)

**Page 3: Data Explorer** 📊
- Tableau interactif avec filtres
- Colonnes: date, market, spec_net_long, open_interest, days_to_expiry, positioning_ratio, pos_score, time_weight, roll_pressure, ALERTE_48H
- Export CSV/Excel

**Page 4: Configuration** ⚙️
- Édition du fichier config.yaml
- Sauvegarde et reset

### 7.3 Mode Test
```bash
# Lancer tous les tests (63 tests)
make test

# Tests avec coverage
pytest --cov=src --cov-report=html

# Tests spécifiques
pytest tests/test_roll_pressure.py -v
```

* * *

## 8. Sorties Générées

Le pipeline génère automatiquement 3 fichiers dans `output/`:

### 8.1 Excel (`roll_pressure_latest.xlsx`)
Fichier Excel multi-feuilles avec:
- **Sheet "Summary"**: Banner avec métriques clés + alertes actives (mise en forme rouge)
- **Sheet "Data"**: Données complètes (240 lignes typiques pour 90 jours × 2 markets)
- **Sheet "Heatmap"**: Heatmap visuelle avec mise en forme conditionnelle

**Colonnes de données**:
| Colonne | Description | Type |
|---------|-------------|------|
| date | Date du calcul | Date |
| market | WTI ou BRENT | String |
| spec_net_long | Positions nettes spéculateurs (CFTC) | Float |
| open_interest | Open Interest (CFTC) | Float |
| days_to_expiry | Jours avant expiration | Int |
| positioning_ratio | spec_net_long / open_interest | Float |
| pos_score | Percentile rank (0-1) | Float |
| time_weight | Amplification temporelle (0-1) | Float |
| roll_pressure | Indicateur final (0-1) | Float |
| ALERTE_48H | Alerte déclenchée? | Boolean |

### 8.2 Heatmap PNG (`heatmap_roll_pressure.png`)
- Résolution 150 DPI
- Format: 2 markets × 60 jours (défaut)
- Couleurs: RdYlGn_r (Red-Yellow-Green reversed)
- Annotations: 🟢🟠🔴 selon seuils

### 8.3 Heatmap HTML (`heatmap_roll_pressure.html`)
- Version interactive (Plotly)
- Hover tooltips avec détails
- Zoom/Pan/Export

* * *

## 9. Intégration Excel VBA (Optionnel)

Pour automatiser l'exécution depuis Excel:

```vba
Sub RunRollPressurePipeline()
    Dim pythonPath As String
    Dim scriptPath As String
    Dim command As String

    pythonPath = "C:\Python39\python.exe"  ' Adapter chemin
    scriptPath = ThisWorkbook.Path & "\src\cli.py"

    command = pythonPath & " -m src.cli run --days 90"

    ' Exécuter
    Shell command, vbNormalFocus

    MsgBox "Pipeline lancé! Vérifiez output/roll_pressure_latest.xlsx"
End Sub
```

* * *

## 10. Limites & Considérations

### Limites actuelles

- **Latence CFTC**: Les données COT sont publiées chaque vendredi (T+3 après Tuesday cutoff) → décalage de 3-10 jours
- **Forward-fill**: Les données hebdomadaires sont forward-filled quotidiennement (perte de précision intra-semaine)
- **Markets limités**: Actuellement WTI et Brent uniquement (extensible à d'autres futures)
- **Open Interest**: Dépend de la disponibilité CFTC (fallback Yahoo Finance si besoin)
- **Percentile warming**: Les 252 premiers jours ont un historique incomplet pour le calcul du percentile

### Quotas API

- **CFTC Socrata API**: Publique, pas de token requis, limite 50,000 rows/requête (largement suffisant)
- **Yahoo Finance**: Gratuit, rate limiting possible (backup seulement)

* * *

## 11. Améliorations Futures

Pistes d'amélioration identifiées:

- **Real-time data** → Intégration CME Group API pour OI en temps réel (payant)
- **Machine Learning** → Modèle prédictif pour anticiper les spike de volatilité
- **Backtest engine** → Validation historique des alertes vs réalisé
- **Multi-asset** → Extension à Natural Gas, Gold, Currencies futures
- **API REST** → Exposer les alertes via API pour intégration trading systems
- **Telegram/Email bot** → Notifications push lors des alertes

* * *

## 12. Contact & Contribution

**Auteur**: [ATTILA Guillaume]
**GitHub**: [https://github.com/llaume974](https://github.com/llaume974)

Contributions bienvenues via Pull Requests!

---

*Dernière mise à jour: Novembre 2025*
