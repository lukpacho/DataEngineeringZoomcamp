# ingest_data.py
from __future__ import annotations

import argparse
from time import time
from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


# helper functions -------------------------------------------------------------
DATETIME_COLS: tuple[str, ...] = (
    # yellow
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    # green
    "lpep_pickup_datetime", "lpep_dropoff_datetime",
)


def to_datetime(df: pd.DataFrame, cols: Iterable[str] = DATETIME_COLS) -> pd.DataFrame:
    """Convert any *present* column names in *cols* to pandas datetime."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c])
    return df


def make_engine(user: str, pwd: str, host: str, port: int, db: str) -> Engine:
    url = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url)


# main ingestion ----------------------------------------------------------------
def ingest(
        csv_url: str, table: str, engine: Engine, chunksize: int = 100_000
        ) -> None:
    print(f"reading {csv_url} ...")
    stream = pd.read_csv(
        csv_url,
        compression="infer",
        chunksize=chunksize,
        iterator=True,
    )

    first_chunk = to_datetime(next(stream))
    first_chunk.head(0).to_sql(table, engine, if_exists="replace", index=False)
    first_chunk.to_sql(table, engine, if_exists="append", index=False)

    rows = len(first_chunk)
    t0 = time()

    for i, chunk in enumerate(stream, start=2):
        t_start = time()
        to_datetime(chunk).to_sql(table, engine, if_exists="append", index=False)
        rows += len(chunk)
        print(
            f"      chunk {i:>3} ({len(chunk):,} rows) inserted "
            f"in {time() - t_start:.2f}s — total {rows:,}"
        )

    print(f"[finished]: {rows:,} rows in {time() - t0:.1f}s")


# CLI -------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest CSV into Postgres by chunks")
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--database", required=True, help="Postgres database name")
    p.add_argument("--table", required=True, help="Destination table name")
    p.add_argument("--csv-url", required=True, help="Local path or http(s):// URL")
    p.add_argument("--chunksize", type=int, default=100_000)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    print("--- connecting to Postgres ...")
    with make_engine(
        args.user, args.password, args.host, args.port, args.database
    ).begin() as eng:
        ingest(args.csv_url, args.table, eng, chunksize=args.chunksize)


if __name__ == "__main__":
    main()
