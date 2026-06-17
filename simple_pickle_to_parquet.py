#!/usr/bin/env python3

from pathlib import Path
import argparse
import pandas as pd
import polars as pl


def to_polars(obj) -> pl.DataFrame:
    if isinstance(obj, pl.DataFrame):
        return obj

    if isinstance(obj, pd.DataFrame):
        return pl.from_pandas(obj)

    if isinstance(obj, list):
        return pl.DataFrame(obj)

    if isinstance(obj, dict):
        return pl.DataFrame([obj])

    raise TypeError(f"Unsupported pickle object type: {type(obj)}")


def convert_directory(input_dir: Path, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(input_dir.glob("*.pkl.gz"))

    print(f"Found {len(files)} files")
    print(f"Input : {input_dir}")
    print(f"Output: {output_dir}")

    for pkl_file in files:
        parquet_file = output_dir / pkl_file.name.replace(".pkl.gz", ".parquet")

        if parquet_file.exists():
            print(f"SKIP  {parquet_file.name}")
            continue

        try:
            print(f"LOAD  {pkl_file.name}")

            obj = pd.read_pickle(pkl_file)
            df = to_polars(obj)

            df.write_parquet(
                parquet_file,
                compression="zstd",
                compression_level=7,
                statistics=True,
                row_group_size=100_000,
            )

            print(f"SAVED {parquet_file.name} ({df.height:,} rows)")

            del obj
            del df

        except Exception as e:
            print(f"ERROR {pkl_file.name}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert pickle files to parquet using Polars."
    )

    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)

    args = parser.parse_args()

    convert_directory(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
