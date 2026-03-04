from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry



import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests

BASE_URL = "https://api.worldbank.org/v2"

def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "macro-etl/1.0"})
    return session

def fetch_series(country: str, indicator: str, start: int, end: int, per_page: int) -> Tuple[Dict[str, Any], List[Dict[str, Any]], str]:
    url = f"{BASE_URL}/country/{country}/indicator/{indicator}"
    params = {
        "format": "json",
        "date": f"{start}:{end}",
        "per_page": str(per_page),
    }
    session = make_session()
    r = session.get(url, params=params, timeout=90)

    payload = r.json()
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError(f"Unexpected API response format for {country}/{indicator}")

    meta = payload[0] if isinstance(payload[0], dict) else {}
    rows = payload[1] if isinstance(payload[1], list) else []
    return meta, rows, r.url


def normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        value = row.get("value")
        year = row.get("date")

        if value is None or year is None:
            continue

        try:
            year_i = int(year)
            value_f = float(value)
        except (ValueError, TypeError):
            continue

        country = row.get("country", {}) or {}
        indicator = row.get("indicator", {}) or {}

        out.append(
            {
                "country_id": country.get("id"),
                "country_name": country.get("value"),
                "indicator_id": indicator.get("id"),
                "indicator_name": indicator.get("value"),
                "year": year_i,
                "value": value_f,
            }
        )

    out.sort(key=lambda x: x["year"])
    return out


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["country_id", "country_name", "indicator_id", "indicator_name", "year", "value"]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--country", default="ESP", help="Country code (ISO2 or ISO3). Example: ESP.")
    ap.add_argument("--indicator", default="NY.GDP.MKTP.CD", help="World Bank indicator code.")
    ap.add_argument("--start", type=int, default=1995)
    ap.add_argument("--end", type=int, default=2024)
    ap.add_argument("--per-page", type=int, default=2000)
    ap.add_argument("--outdir", default="data/raw")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    country = args.country.strip()
    indicator = args.indicator.strip()

    meta, rows, final_url = fetch_series(country, indicator, args.start, args.end, args.per_page)

    raw_path = outdir / f"{country}_{indicator}_{args.start}_{args.end}.json"
    csv_path = outdir / f"{country}_{indicator}_{args.start}_{args.end}.csv"

    write_json(raw_path, {"request_url": final_url, "meta": meta, "data": rows})

    normalized = normalize_rows(rows)
    write_csv(csv_path, normalized)

    print(f"Request: {final_url}")
    print(f"Saved raw JSON -> {raw_path}")
    print(f"Saved normalized CSV -> {csv_path}")
    print(f"Rows (non-null): {len(normalized)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
