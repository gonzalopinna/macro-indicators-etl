import argparse
import csv
import json
import time
from pathlib import Path
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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


def fetch_series(session: requests.Session, country: str, indicator: str, start: int, end: int, per_page: int) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/country/{country}/indicator/{indicator}"
    params = {"format": "json", "date": f"{start}:{end}", "per_page": str(per_page)}
    r = session.get(url, params=params, timeout=(10, 90))
    r.raise_for_status()

    payload = r.json()
    if not isinstance(payload, list) or len(payload) < 2:
        return []

    rows = payload[1] if isinstance(payload[1], list) else []
    return rows


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

    out.sort(key=lambda x: (x["country_id"], x["indicator_id"], x["year"]))
    return out


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["country_id", "country_name", "indicator_id", "indicator_name", "year", "value"]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--countries", default="ES,FR,DE,IT,PT,GB,US,JP")
    ap.add_argument("--indicators", default="NY.GDP.MKTP.CD,NY.GDP.PCAP.CD,FP.CPI.TOTL.ZG,SL.UEM.TOTL.ZS,NY.GDP.MKTP.KD.ZG")
    ap.add_argument("--start", type=int, default=1995)
    ap.add_argument("--end", type=int, default=2024)
    ap.add_argument("--per-page", type=int, default=200)
    ap.add_argument("--outdir", default="data/raw")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    countries = [c.strip() for c in args.countries.split(",") if c.strip()]
    indicators = [i.strip() for i in args.indicators.split(",") if i.strip()]

    session = make_session()

    all_rows: List[Dict[str, Any]] = []
    raw_meta = {"countries": countries, "indicators": indicators, "start": args.start, "end": args.end}

    for c in countries:
        for ind in indicators:
            try:
                rows = fetch_series(session, c, ind, args.start, args.end, args.per_page)
            except requests.HTTPError as exc:
                print(f"Skipping {c} / {ind}: {exc}")
                continue
            norm = normalize_rows(rows)
            all_rows.extend(norm)
            print(f"{c} / {ind}: {len(norm)} rows")

            # be gentle with the API
            time.sleep(0.2)

    # Save consolidated CSV
    out_csv = outdir / f"observations_{args.start}_{args.end}.csv"
    write_csv(out_csv, all_rows)

    # Save metadata
    write_json(outdir / f"observations_{args.start}_{args.end}_meta.json", raw_meta)

    print(f"Saved consolidated CSV -> {out_csv}")
    print(f"Total rows: {len(all_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
