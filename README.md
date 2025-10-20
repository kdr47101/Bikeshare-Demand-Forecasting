# Station-hour demand forecasting (2024)

## Dataset
- **Source:** Bike Share Toronto Ridership (API, JSON), filtered to **2024** only.
- **Grain:** **station × hour** (local time).
- **Storage:** Raw API snapshots → `data/raw/`; curated tables → `data/interim/`, `data/processed/`.

## Business Value
- Hourly station-level forecasts improve **rebalancing**, **maintenance scheduling**, and **dock/bike availability**, reducing stockouts/overflows and improving customer experience.

## Analysis (minimal, <1 week)
- Baseline model (seasonal naive or SARIMAX) with simple temporal features (hour-of-day, day-of-week, lags t-24/t-168, holidays).
- Hold out last weeks of 2024 for honest MAE/MAPE; keep runtime short and artifacts reproducible.

## Dashboard (Power BI)
- **KPIs:** MAE/MAPE, predicted vs. actual.
- **Map:** station color by forecasted risk; tooltip shows next 24–72h.
- **Station page:** forecast vs. actual with intervals.
- **Planning table:** next-day hour-by-hour forecast per station.
- Power BI imports curated CSVs from `reports/powerbi/dataset/`.

## Pipeline & File Flow
1. `src/get_data.py` → pulls 2024 JSON → `data/raw/`
2. `src/build_station_hour.py` ← raw → clean & normalize to **station×hour** → `data/interim/`
3. `src/make_features.py` ← interim → features/targets → `data/processed/`
4. `src/train.py` ← processed → trains baseline → artifacts in `models/artifacts/`
5. `src/forecast.py` ← artifacts + processed → forecasts CSV → `data/processed/` and/or `reports/powerbi/dataset/`
6. `src/export_for_bi.py` ← actuals + forecasts → `fact_*` and `dim_*` CSVs → `reports/powerbi/dataset/`
7. Power BI file `reports/powerbi/StationHour_Forecast_2024.pbix` ← curated CSVs

## Run Order (after you add code)
1. `python src/get_data.py`
2. `python src/build_station_hour.py`
3. `python src/make_features.py`
4. `python src/train.py`
5. `python src/forecast.py`
6. `python src/export_for_bi.py`

## Repo Layout (essentials only)
- `README.md` – these docs (dataset, business value, analysis, dashboard, flow)
- `.gitignore` – exclude large data/models, PBIX
- `requirements.txt` – minimal deps
- `.env.example` – config template
- `data/` – raw/interim/processed
- `models/artifacts/` – trained model + preprocessors
- `reports/figs/` – optional EDA plots
- `reports/powerbi/` – PBIX + curated dataset CSVs
- `src/` – pipeline scripts (see below)
- `notebooks/` – optional EDA/error-checks

## Source Scripts (no code yet—just roles)
- `src/config.py` – central config (year=2024, paths, endpoints, feature flags, model type/horizon)
- `src/get_data.py` – API client → `data/raw/`
- `src/build_station_hour.py` – normalize/clean to station×hour → `data/interim/`
- `src/make_features.py` – add features/target → `data/processed/`
- `src/train.py` – train baseline → `models/artifacts/`
- `src/forecast.py` – generate forecasts CSV → `data/processed/` or `reports/powerbi/dataset/`
- `src/export_for_bi.py` – write `fact_*` & `dim_*` CSVs → `reports/powerbi/dataset/`

