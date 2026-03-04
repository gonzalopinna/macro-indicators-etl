import argparse
import sqlite3
from pathlib import Path
from typing import List, Tuple, Optional


def q1(con: sqlite3.Connection, sql: str, params=()) -> Optional[Tuple]:
    cur = con.execute(sql, params)
    return cur.fetchone()


def qall(con: sqlite3.Connection, sql: str, params=()) -> List[Tuple]:
    cur = con.execute(sql, params)
    return cur.fetchall()


def get_snapshot(con: sqlite3.Connection):
    obs = q1(con, "SELECT COUNT(*) FROM observations;")[0]
    countries = q1(con, "SELECT COUNT(*) FROM countries;")[0]
    indicators = q1(con, "SELECT COUNT(*) FROM indicators;")[0]
    yr_min, yr_max = q1(con, "SELECT MIN(year), MAX(year) FROM observations;")
    return obs, countries, indicators, yr_min, yr_max


def pick_comparison_year(con: sqlite3.Connection, indicator_id: str, total_countries: int, min_cov_ratio: float) -> Tuple[int, int]:
    """
    Pick the most recent year with coverage >= threshold if possible.
    Otherwise, pick the year with maximum coverage (ties -> most recent).
    Returns: (year, coverage_count)
    """
    rows = qall(
        con,
        """
        SELECT year, COUNT(DISTINCT country_id) AS cov
        FROM observations
        WHERE indicator_id = ?
        GROUP BY year
        ORDER BY year DESC;
        """,
        (indicator_id,),
    )
    if not rows:
        return (0, 0)

    threshold = max(1, int(total_countries * min_cov_ratio))

    for year, cov in rows:
        if cov >= threshold:
            return int(year), int(cov)

    # fallback: year with max coverage, tie -> most recent
    best_year, best_cov = max(rows, key=lambda x: (x[1], x[0]))
    return int(best_year), int(best_cov)


def get_indicator_list(con: sqlite3.Connection) -> List[Tuple[str, str]]:
    return [(r[0], r[1]) for r in qall(con, "SELECT indicator_id, indicator_name FROM indicators ORDER BY indicator_name;")]


def get_rank_for_year(con: sqlite3.Connection, indicator_id: str, year: int) -> List[Tuple[str, str, float]]:
    return [
        (r[0], r[1], float(r[2]))
        for r in qall(
            con,
            """
            SELECT c.country_id, c.country_name, o.value
            FROM observations o
            JOIN countries c ON c.country_id = o.country_id
            WHERE o.indicator_id = ? AND o.year = ?
            ORDER BY o.value DESC;
            """,
            (indicator_id, year),
        )
    ]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/wb.sqlite")
    ap.add_argument("--out", default="reports/compare_summary.md")
    ap.add_argument("--top", type=int, default=10, help="Top N countries to show")
    ap.add_argument("--min-cov", type=float, default=0.8, help="Min coverage ratio for choosing comparison year (0-1)")
    args = ap.parse_args()

    db_path = Path(args.db)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(db_path)
    try:
        obs, n_countries, n_indicators, yr_min, yr_max = get_snapshot(con)
        indicators = get_indicator_list(con)

        lines: List[str] = []
        lines.append("# Macro Indicators Comparative Report\n\n")
        lines.append("## Dataset snapshot\n")
        lines.append(f"- Observations: **{obs}**\n")
        lines.append(f"- Countries: **{n_countries}**\n")
        lines.append(f"- Indicators: **{n_indicators}**\n")
        lines.append(f"- Year range: **{yr_min} – {yr_max}**\n\n")

        lines.append("## Rankings by indicator\n")
        lines.append(
            "> Comparison year is chosen as the most recent year with high country coverage; if not available, it falls back to the year with maximum coverage.\n\n"
        )

        for indicator_id, indicator_name in indicators:
            year, cov = pick_comparison_year(con, indicator_id, n_countries, args.min_cov)
            if year == 0:
                continue

            rank_rows = get_rank_for_year(con, indicator_id, year)
            if not rank_rows:
                continue

            lines.append(f"### {indicator_name}\n")
            lines.append(f"- Indicator ID: `{indicator_id}`\n")
            lines.append(f"- Comparison year: **{year}** (coverage: **{cov}/{n_countries}** countries)\n\n")

            top_n = min(args.top, len(rank_rows))
            lines.append(f"Top {top_n}:\n\n")
            lines.append("| Rank | Country | Value |\n")
            lines.append("|---:|---|---:|\n")
            for i, (_, country_name, value) in enumerate(rank_rows[:top_n], start=1):
                lines.append(f"| {i} | {country_name} | {value:,.4f} |\n")
            lines.append("\n")

            # Bottom N (useful in many indicators)
            bottom_n = min(args.top, len(rank_rows))
            lines.append(f"Bottom {bottom_n}:\n\n")
            lines.append("| Rank | Country | Value |\n")
            lines.append("|---:|---|---:|\n")
            bottom = list(reversed(rank_rows[-bottom_n:]))
            for i, (_, country_name, value) in enumerate(bottom, start=1):
                lines.append(f"| {i} | {country_name} | {value:,.4f} |\n")
            lines.append("\n---\n\n")

        out_path.write_text("".join(lines), encoding="utf-8")
        print(f"Saved comparative report -> {out_path}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
