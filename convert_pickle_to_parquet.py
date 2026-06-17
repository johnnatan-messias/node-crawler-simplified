#!/usr/bin/env python3
"""
Command-line tool for converting blockchain pickle files to Parquet format.

Usage:
    python convert_pickle_to_parquet.py \\
        --blocks-dir ./data/blocks \\
        --receipts-dir ./data/blocks_receipts \\
        --output-dir ./parquet_output \\
        --compression zstd

Author: Johnnatan Messias
https://johnnatan-messias.github.io
https://www.linkedin.com/in/johnnatan-messias/
https://twitter.com/johnnatan_me
https://scholar.google.com/citations?user=EoGEeFAAAAAJ
"""

import argparse
import logging
import sys
from pathlib import Path

from pickle_to_parquet import PickleToParquetConverter


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def main():
    parser = argparse.ArgumentParser(
        description="Convert blockchain pickle files to Parquet format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert blocks and receipts
  python convert_pickle_to_parquet.py \\
    --blocks-dir ./data/blocks \\
    --receipts-dir ./data/blocks_receipts

  # Convert with custom output directory and compression
  python convert_pickle_to_parquet.py \\
    --blocks-dir ./data/blocks \\
    --receipts-dir ./data/blocks_receipts \\
    --output-dir ./parquet_output \\
    --compression gzip

  # Convert only blocks
  python convert_pickle_to_parquet.py \\
    --blocks-dir ./data/blocks \\
    --blocks-only

  # Convert a directory of transaction pickle files
  python convert_pickle_to_parquet.py \\
    --transactions-dir ./data/transactions \\
    --transactions-only
        """
    )

    parser.add_argument(
        "--blocks-dir",
        type=str,
        required=False,
        default=None,
        help="Directory containing block pickle files (e.g., ./data/timeboost/blocks)"
    )
    parser.add_argument(
        "--receipts-dir",
        type=str,
        required=False,
        default=None,
        help="Directory containing receipt pickle files (e.g., ./data/timeboost/blocks_receipts)"
    )
    parser.add_argument(
        "--transactions-dir",
        type=str,
        required=False,
        default=None,
        help="Directory containing transaction pickle files (e.g., ./data/timeboost/transactions)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./parquet_output",
        help="Output directory for Parquet files (default: ./parquet_output)"
    )
    parser.add_argument(
        "--compression",
        type=str,
        default="zstd",
        choices=["snappy", "gzip", "brotli", "zstd", "none"],
        help="Compression codec (default: zstd)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker processes to use (default: auto)"
    )

    only_group = parser.add_mutually_exclusive_group()
    only_group.add_argument(
        "--blocks-only",
        action="store_true",
        help="Convert only blocks (skip receipts)"
    )
    only_group.add_argument(
        "--receipts-only",
        action="store_true",
        help="Convert only receipts (skip blocks)"
    )
    only_group.add_argument(
        "--transactions-only",
        action="store_true",
        help="Convert only transactions (skip blocks and receipts)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    output_dir = Path(args.output_dir) if args.output_dir else None

    # Initialize converter
    try:
        converter = PickleToParquetConverter(output_dir=output_dir)
    except Exception as e:
        logger.error(f"Failed to initialize converter: {e}")
        sys.exit(1)

    def resolve_input_dir(value: str, label: str, required: bool = False) -> Path | None:
        if not value:
            if required:
                logger.error(f"--{label}-dir is required for this conversion")
                sys.exit(1)
            return None

        path = Path(value)
        if not path.exists():
            logger.error(f"{label.capitalize()} directory not found: {path}")
            sys.exit(1)
        if not path.is_dir():
            logger.error(f"{label.capitalize()} path is not a directory: {path}")
            sys.exit(1)
        return path

    blocks_dir = resolve_input_dir(args.blocks_dir, "blocks", args.blocks_only)
    receipts_dir = resolve_input_dir(args.receipts_dir, "receipts", args.receipts_only)
    transactions_dir = resolve_input_dir(
        args.transactions_dir, "transactions", args.transactions_only)

    conversions = []
    if args.blocks_only:
        conversions.append(("blocks", blocks_dir, converter.convert_blocks, "blocks"))
    elif args.receipts_only:
        conversions.append(("receipts", receipts_dir, converter.convert_receipts, "blocks_receipts"))
    elif args.transactions_only:
        conversions.append(("transactions", transactions_dir,
                           converter.convert_transactions, "transactions"))
    else:
        if blocks_dir:
            conversions.append(("blocks", blocks_dir, converter.convert_blocks, "blocks"))
        if receipts_dir:
            conversions.append(("receipts", receipts_dir,
                               converter.convert_receipts, "blocks_receipts"))
        if transactions_dir:
            conversions.append(("transactions", transactions_dir,
                               converter.convert_transactions, "transactions"))

    if not conversions:
        logger.error(
            "No input directories provided. Use --blocks-dir, --receipts-dir, "
            "or --transactions-dir."
        )
        sys.exit(1)

    # Perform conversion
    try:
        summary = {}
        for label, input_dir, convert_func, output_subdir in conversions:
            logger.info(f"Converting {label} from {input_dir}...")
            summary[label] = convert_func(
                input_dir=input_dir,
                output_dir=converter.output_dir / output_subdir,
                compression=args.compression,
                max_workers=args.workers,
            )

        for label, (successful, failed) in summary.items():
            logger.info(f"{label.capitalize()}: {successful} ✓, {failed} ✗")

        total_failed = sum(failed for _, failed in summary.values())
        if total_failed:
            logger.error(f"Conversion finished with {total_failed} failed file(s)")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("Conversion interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Conversion failed: {e}", exc_info=args.verbose)
        sys.exit(1)

    logger.info("✓ Conversion completed successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()
