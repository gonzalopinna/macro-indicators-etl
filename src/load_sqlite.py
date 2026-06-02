from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Dict


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS countries (
  country_id   TEXT PRIMARY KEY,
  country_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS indicators (
  indicator_id   TEXT PRIMARY KEY,
  indicator_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS observations (
  country_id   TEXT NOT NULL,
  indicator_id TEXT NOT NULL,
  year         INTEGER NOT NULL,
  value        REAL NOT NULL,
  PRIMARY KEY (country_id, indicator_id, year),
  FOREIGN KEY (country_id) REFERENCES countries(country_id),
  FOREIGN KEY (indicator_id) REFERENCES indicators(indicator_id)
);

CREATE INDEX IF NOT EXISTS idx_obs_indicator_year ON observations(indicator_id, year);
CREATE INDEX IF NOT EXISTS idx_obs_country_year   ON observations(country_id, year);
"""


def upsert_country(cur: sqlite3.Cursor, country_id: str, country_name: str) -> None:
    cur.execute(
        """
        INSERT INTO countries(country_id, country_name)
        VALUES (?, ?)
        ON CONFLICT(country_id) DO UPDATE SET country_name = excluded.country_name
        """,
        (country_id, country_name),
    )


def upsert_indicator(cur: sqlite3.Cursor, indicator_id: str, indicator_name: str) -> None:
    cur.execute(
        """
        INSERT INTO indicators(indicator_id, indicator_name)
        VALUES (?, ?)
        ON CONFLICT(indicator_id) DO UPDATE SET indicator_name = excluded.indicator_name
        """,
        (indicator_id, indicator_name),
    )


def upsert_observation(cur: sqlite3.Cursor, row: Dict[str, str]) -> None:
    cur.execute(
        """
        INSERT INTO observations(country_id, indicator_id, year, value)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(country_id, indicator_id, year) DO UPDATE SET value = excluded.value
        """,
        (row["country_id"], row["indicator_id"], int(row["year"]), float(row["value"])),
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to normalized CSV produced by 01_extract_worldbank.py")
    ap.add_argument("--db", default="data/wb.sqlite", help="SQLite DB path")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA foreign_keys = ON;")
        con.executescript(SCHEMA_SQL)

        n = 0
        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cur = con.cursor()

            for row in reader:
                # Basic validation
                if not row.get("country_id") or not row.get("indicator_id") or not row.get("year") or not row.get("value"):
                    continue

                upsert_country(cur, row["country_id"], row.get("country_name", "").strip() or row["country_id"])
                upsert_indicator(cur, row["indicator_id"], row.get("indicator_name", "").strip() or row["indicator_id"])
                upsert_observation(cur, row)
                n += 1

            con.commit()

        # Quick sanity checks
        cur = con.cursor()
        obs_count = cur.execute("SELECT COUNT(*) FROM observations;").fetchone()[0]
        years = cur.execute("SELECT MIN(year), MAX(year) FROM observations;").fetchone()
        print(f"DB: {db_path}")
        print(f"Loaded rows from CSV: {n}")
        print(f"Observations in DB: {obs_count}")
        print(f"Year range in DB: {years[0]} - {years[1]}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
