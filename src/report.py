from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Optional, Tuple


def fetch_latest(con: sqlite3.Connection, country: str, indicator: str) -> Optional[Tuple[int, float]]:
    row = con.execute(
        """
        SELECT year, value
        FROM observations
        WHERE country_id = ? AND indicator_id = ?
        ORDER BY year DESC
        LIMIT 1
        """,
        (country, indicator),
    ).fetchone()
    return (int(row[0]), float(row[1])) if row else None


def fetch_value_at_year(con: sqlite3.Connection, country: str, indicator: str, year: int) -> Optional[float]:
    row = con.execute(
        """
        SELECT value
        FROM observations
        WHERE country_id = ? AND indicator_id = ? AND year = ?
        """,
        (country, indicator, year),
    ).fetchone()
    return float(row[0]) if row else None


def fetch_decade_averages(con: sqlite3.Connection, country: str, indicator: str):
    # decade = floor(year/10)*10
    rows = con.execute(
        """
        SELECT (year / 10) * 10 AS decade, AVG(value) AS avg_value, COUNT(*) AS n_years
        FROM observations
        WHERE country_id = ? AND indicator_id = ?
        GROUP BY decade
        ORDER BY decade
        """,
        (country, indicator),
    ).fetchall()
    return [(int(d), float(avgv), int(n)) for (d, avgv, n) in rows]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/wb.sqlite")
    ap.add_argument("--country", default="ES")
    ap.add_argument("--indicator", default="NY.GDP.MKTP.CD")
    ap.add_argument("--out", default="reports/summary.md")
    args = ap.parse_args()

    db_path = Path(args.db)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(db_path)
    try:
        latest = fetch_latest(con, args.country, args.indicator)
        if not latest:
            raise SystemExit("No data found for given country/indicator.")

        latest_year, latest_value = latest
        v_5y = fetch_value_at_year(con, args.country, args.indicator, latest_year - 5)
        v_10y = fetch_value_at_year(con, args.country, args.indicator, latest_year - 10)

        # compute deltas if possible
        def pct_change(old: Optional[float], new: float) -> Optional[float]:
            if old is None or old == 0:
                return None
            return (new / old - 1.0) * 100.0

        chg_5y = pct_change(v_5y, latest_value)
        chg_10y = pct_change(v_10y, latest_value)

        decades = fetch_decade_averages(con, args.country, args.indicator)

        # Render markdown
        lines = []
        lines.append("# Macro Indicators Report\n")
        lines.append(f"**Country**: {args.country}\n")
        lines.append(f"**Indicator**: {args.indicator}\n")

        lines.append("## Latest\n")
        lines.append(f"- Latest year: **{latest_year}**\n")
        lines.append(f"- Latest value: **{latest_value:,.2f}**\n")

        lines.append("\n## Change\n")
        if v_5y is not None and chg_5y is not None:
            lines.append(f"- 5Y change ({latest_year-5} → {latest_year}): **{chg_5y:.2f}%**\n")
        else:
            lines.append("- 5Y change: N/A\n")

        if v_10y is not None and chg_10y is not None:
            lines.append(f"- 10Y change ({latest_year-10} → {latest_year}): **{chg_10y:.2f}%**\n")
        else:
            lines.append("- 10Y change: N/A\n")

        lines.append("\n## Decade averages\n")
        lines.append("| Decade | Avg value | Years |\n")
        lines.append("|---:|---:|---:|\n")
        for decade, avg_value, n in decades:
            lines.append(f"| {decade}s | {avg_value:,.2f} | {n} |\n")

        out_path.write_text("".join(lines), encoding="utf-8")
        print(f"Saved report -> {out_path}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
