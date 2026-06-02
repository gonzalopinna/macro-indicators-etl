# Macro Indicators ETL (World Bank)

This project is a small **ETL pipeline** that pulls macroeconomic indicators from the World Bank Indicators **REST API**, cleans and normalizes the data, loads it into a local **SQLite** database, and generates Markdown reports. It includes a batch mode to fetch multiple countries/indicators and produce simple comparative rankings.

## Tech Stack
- Python
- requests
- SQLite (sqlite3)

## Project Structure
- `src/`
  - `extract_worldbank.py` - Extract a single country + single indicator (raw JSON + normalized CSV)
  - `extract_batch_worldbank.py` - Batch extract (multiple countries/indicators into one consolidated CSV)
  - `load_sqlite.py` - Load normalized CSV into `data/wb.sqlite` (idempotent upsert)
  - `report.py` - Generate a single-series report (`reports/summary.md`)
  - `report_compare.py` - Generate a comparative report (`reports/compare_summary.md`)

## Setup
```bash
python -m pip install -r requirements.txt
```

## Run (Single Indicator)
```bash
python src/extract_worldbank.py --country ES --indicator NY.GDP.MKTP.CD --start 1995 --end 2024 --per-page 200
python src/load_sqlite.py --csv "data/raw/ES_NY.GDP.MKTP.CD_1995_2024.csv"
python src/report.py --db data/wb.sqlite --country ES --indicator NY.GDP.MKTP.CD
```

## Run (Batch + Rankings)
```bash
python src/extract_batch_worldbank.py --start 1995 --end 2024
python src/load_sqlite.py --csv "data/raw/observations_1995_2024.csv"
python src/report_compare.py --db data/wb.sqlite --out reports/compare_summary.md
```

## Outputs
- `data/raw/` - Raw JSON + normalized CSV files (generated locally)
- `data/wb.sqlite` - SQLite database (generated locally)
- `reports/summary.md` - Single-series summary report
- `reports/compare_summary.md` - Comparative rankings report (requires multiple countries)

## Notes
I do not commit `data/` (raw dumps and the local SQLite DB). The scripts generate everything locally.

The load step is idempotent: re-running it updates existing rows instead of duplicating them.
