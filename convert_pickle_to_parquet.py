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
  # Convert blocks and receipts with default output directory
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
    --receipts-dir ./data/blocks_receipts \\
    --blocks-only
        """
    )

    parser.add_argument(
        "--blocks-dir",
        type=str,
        required=True,
        help="Directory containing block pickle files (e.g., ./data/timeboost/blocks)"
    )
    parser.add_argument(
        "--receipts-dir",
        type=str,
        required=True,
        help="Directory containing receipt pickle files (e.g., ./data/timeboost/blocks_receipts)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
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
    parser.add_argument(
        "--blocks-only",
        action="store_true",
        help="Convert only blocks (skip receipts)"
    )
    parser.add_argument(
        "--receipts-only",
        action="store_true",
        help="Convert only receipts (skip blocks)"
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

    # Validate arguments
    if args.blocks_only and args.receipts_only:
        logger.error("Cannot use both --blocks-only and --receipts-only")
        sys.exit(1)

    # Validate input directories
    blocks_dir = Path(args.blocks_dir)
    receipts_dir = Path(args.receipts_dir)

    if not blocks_dir.exists():
        logger.error(f"Blocks directory not found: {blocks_dir}")
        sys.exit(1)

    if not receipts_dir.exists():
        logger.error(f"Receipts directory not found: {receipts_dir}")
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else None

    # Initialize converter
    try:
        converter = PickleToParquetConverter(output_dir=output_dir)
    except Exception as e:
        logger.error(f"Failed to initialize converter: {e}")
        sys.exit(1)

    # Perform conversion
    try:
        if args.receipts_only:
            logger.info("Converting receipts only...")
            converter.convert_receipts(
                input_dir=receipts_dir,
                compression=args.compression,
                max_workers=args.workers,
            )
        elif args.blocks_only:
            logger.info("Converting blocks only...")
            converter.convert_blocks(
                input_dir=blocks_dir,
                compression=args.compression,
                max_workers=args.workers,
            )
        else:
            logger.info("Converting blocks and receipts...")
            summary = converter.convert_all(
                blocks_dir=blocks_dir,
                receipts_dir=receipts_dir,
                output_dir=converter.output_dir
            )

            # Print detailed summary
            if not args.blocks_only:
                logger.info(
                    f"Blocks: {summary['blocks'][0]} ✓, {summary['blocks'][1]} ✗")
            if not args.receipts_only:
                logger.info(
                    f"Receipts: {summary['receipts'][0]} ✓, {summary['receipts'][1]} ✗")

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
