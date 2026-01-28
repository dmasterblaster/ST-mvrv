import os
import io
import json
from pathlib import Path

import pandas as pd
import requests


API_URL = "https://api.bitcoinmagazinepro.com/metrics/short-term-holder-mvrv"
# If needed:
# API_URL = "https://api.bitcoinmagazinepro.com/v1/metrics/short-term-holder-mvrv"


def _normalize_cols(df: pd.DataFrame) -> dict:
    """Map lowercase column name -> original column name."""
    return {str(c).strip().lower(): c for c in df.columns}


def _pick_mvrv_column(df: pd.DataFrame) -> str:
    """
    Try to find the MVRV column robustly, since BMP endpoints can vary.
    Rules:
      - exclude obvious non-mvrv columns: Date, Price, MarketCap, etc.
      - prefer columns containing 'mvrv'
      - otherwise, pick the first numeric-looking non-date/non-price column
    """
    colmap = _normalize_cols(df)

    # Common exclusions
    excluded = {
        "date",
        "time",
        "timestamp",
        "price",
        "marketcap",
        "market_cap",
        "realized_cap",
        "realizedcap",
    }

    # 1) Prefer anything with "mvrv" in name
    mvrv_candidates = []
    for lc, orig in colmap.items():
        if lc in excluded:
            continue
        if "mvrv" in lc:
            mvrv_candidates.append(orig)

    if mvrv_candidates:
        # Prefer something that also hints "short" or "sth" if available
        preferred = None
        for c in mvrv_candidates:
            lc = str(c).strip().lower()
            if "short" in lc or "sth" in lc:
                preferred = c
                break
        return preferred or mvrv_candidates[0]

    # 2) Fallback: first non-date/non-price column
    for lc, orig in colmap.items():
        if lc in excluded:
            continue
        return orig

    raise RuntimeError("Could not determine MVRV column from response")


def main():
    api_key = os.environ["BMP_API_KEY"]

    headers = {
        "Authorization": f"Bearer {api_key}",
    }

    resp = requests.get(API_URL, headers=headers, timeout=30)
    print("BMP API status code:", resp.status_code)
    resp.raise_for_status()

    raw_text = resp.text
    if not raw_text.strip():
        raise RuntimeError("Empty response from BMP API")

    # Response is often a quoted string with literal "\n"
    csv_quoted = raw_text.strip()
    if csv_quoted.startswith('"') and csv_quoted.endswith('"'):
        csv_quoted = csv_quoted[1:-1]

    csv_text = csv_quoted.replace("\\n", "\n")

    df = pd.read_csv(io.StringIO(csv_text))
    if df.empty:
        raise RuntimeError("Parsed empty DataFrame")

    print("Parsed columns:", list(df.columns))

    if "Date" not in df.columns and "date" not in [str(c).lower() for c in df.columns]:
        raise RuntimeError(f"Expected a 'Date' column, got {list(df.columns)}")

    # Normalize Date column name if needed
    colmap = _normalize_cols(df)
    date_col = colmap.get("date", "Date")

    mvrv_col = _pick_mvrv_column(df)

    # Price is optional but nice to include if present
    price_col = colmap.get("price", None)

    keep_cols = [date_col, mvrv_col]
    if price_col is not None:
        keep_cols.append(price_col)

    df = df[keep_cols].copy()

    df[date_col] = df[date_col].astype(str)
    df[mvrv_col] = pd.to_numeric(df[mvrv_col], errors="coerce")
    if price_col is not None:
        df[price_col] = pd.to_numeric(df[price_col], errors="coerce")

    df = df.dropna(subset=[date_col, mvrv_col])

    data = []
    for _, row in df.iterrows():
        item = {
            "date": row[date_col],
            "sth_mvrv": float(row[mvrv_col]),
        }
        if price_col is not None and pd.notna(row[price_col]):
            item["price"] = float(row[price_col])
        data.append(item)

    out_path = Path("data/short-term-holder-mvrv.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, indent=2))

    print(f"Wrote {len(data)} points to {out_path}")
    print(f"Using MVRV column: {mvrv_col}")
    if price_col is not None:
        print(f"Including Price column: {price_col}")


if __name__ == "__main__":
    main()
