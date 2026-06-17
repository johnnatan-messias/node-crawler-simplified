"""
Utility module for converting pickle files to Parquet format.

This module provides functionality to convert blockchain data stored in
compressed pickle files (.pkl.gz) to efficient Parquet format for analysis.

Author: Johnnatan Messias
https://johnnatan-messias.github.io
https://www.linkedin.com/in/johnnatan-messias/
https://twitter.com/johnnatan_me
https://scholar.google.com/citations?user=EoGEeFAAAAAJ
"""
import os
import gzip
import pickle
import logging
from pathlib import Path
from typing import Union, Optional, List, Dict, Tuple
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

import polars as pl
from tqdm import tqdm


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PickleToParquetConverter:
    """Convert blockchain pickle files to Parquet format."""

    INFER_SCHEMA_COLUMNS = {'logs', 'accessList', 'authorizationList'}

    # Data type mappings for efficient storage
    BLOCK_SCHEMA = {
        'number': pl.UInt64,
        'hash': pl.Utf8,
        'parentHash': pl.Utf8,
        'miner': pl.Utf8,
        'difficulty': pl.UInt64,
        'totalDifficulty': pl.UInt64,
        'size': pl.UInt32,
        'gasLimit': pl.UInt64,
        'gasUsed': pl.UInt64,
        'timestamp': pl.Datetime(time_unit='ms'),
        # 'transactions': pl.List(pl.Utf8),
        'uncles': pl.List(pl.Utf8),
    }

    RECEIPT_SCHEMA = {
        'blockNumber': pl.UInt64,
        'transactionHash': pl.Utf8,
        'transactionIndex': pl.UInt32,
        'blockHash': pl.Utf8,
        'from': pl.Utf8,
        'to': pl.Utf8,
        'cumulativeGasUsed': pl.UInt64,
        'gasUsed': pl.UInt64,
        'contractAddress': pl.Utf8,
        'logs': pl.List(pl.Struct([
            pl.Field('address', pl.Utf8),
            pl.Field('topics', pl.List(pl.Utf8)),
            pl.Field('data', pl.Utf8),
            # pl.Field('blockNumber', pl.UInt64),
            # pl.Field('blockHash', pl.Utf8),
            # pl.Field('transactionHash', pl.Utf8),
            # pl.Field('transactionIndex', pl.UInt32),
            pl.Field('logIndex', pl.UInt32),
            pl.Field('removed', pl.Boolean),
        ])),
        'status': pl.Boolean,
        'timeboosted': pl.Boolean,
    }

    TRANSACTION_SCHEMA = {
        'blockNumber': pl.Int64,
        'hash': pl.Binary,
        'blockHash': pl.Binary,
        'transactionIndex': pl.Int64,
        'type': pl.Int64,
        'chainId': pl.Int64,
        'nonce': pl.Int64,
        'gas': pl.Int64,
        'maxFeePerGas': pl.Int64,
        'maxPriorityFeePerGas': pl.Int64,
        'to': pl.Utf8,
        'value': pl.Int128,
        'accessList': pl.List(pl.Struct([
            pl.Field('address', pl.Utf8),
            pl.Field('storageKeys', pl.List(pl.Utf8)),
        ])),
        'input': pl.Binary,
        'r': pl.Binary,
        's': pl.Binary,
        'yParity': pl.Int64,
        'v': pl.Int64,
        'from': pl.Utf8,
        'gasPrice': pl.Int64,
        'sourceHash': pl.Utf8,
        'mint': pl.Utf8,
        'depositReceiptVersion': pl.Utf8,
        'authorizationList': pl.List(pl.Struct([
            pl.Field('chainId', pl.Int64),
            pl.Field('address', pl.Utf8),
            pl.Field('nonce', pl.Int64),
            pl.Field('yParity',	pl.Int64),
            pl.Field('r',	pl.Binary),
            pl.Field('s',	pl.Binary)
        ]))
    }

    def __init__(self, output_dir: Optional[Union[str, Path]] = None):
        """Initialize converter with optional output directory.

        Args:
            output_dir: Directory to save Parquet files. If None, creates
                       'parquet_output' in current directory.
        """
        self.output_dir = Path(output_dir or Path.cwd() / "parquet_output")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def load_pickle_file(self, file_path: Union[str, Path]) -> List:
        """Load data from a gzipped pickle file.

        Args:
            file_path: Path to the .pkl.gz file

        Returns:
            List: Loaded data from the pickle file

        Raises:
            FileNotFoundError: If file doesn't exist
            pickle.UnpicklingError: If file is corrupted
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            with gzip.open(file_path, 'rb') as f:
                data = pickle.load(f)
            return data
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            raise

    @staticmethod
    def _convert_value(value):
        """Convert Web3 types to standard Python types.

        Args:
            value: Value to convert (may be HexBytes, AttributeDict, etc.)

        Returns:
            Standard Python type
        """
        if value is None:
            return None
        # Handle HexBytes and similar types
        if hasattr(value, 'hex'):
            return value.hex()
        # Handle AttributeDict and dict-like objects recursively
        if hasattr(value, 'items'):
            try:
                return {
                    key: PickleToParquetConverter._convert_value(item)
                    for key, item in dict(value).items()
                }
            except:
                return str(value)
        if isinstance(value, (list, tuple)):
            return [PickleToParquetConverter._convert_value(item) for item in value]
        return value

    @staticmethod
    def _dataframe_with_known_types(normalized: List[Dict], schema: Dict) -> pl.DataFrame:
        """Create a DataFrame without dropping fields that are not in schema."""
        if not normalized:
            return pl.DataFrame(schema=schema)

        df = pl.DataFrame(normalized, infer_schema_length=None)
        for column, dtype in schema.items():
            if column not in df.columns or column in PickleToParquetConverter.INFER_SCHEMA_COLUMNS:
                continue
            try:
                df = df.with_columns(pl.col(column).cast(dtype, strict=False))
            except Exception as e:
                logger.warning(f"Keeping inferred type for column {column}: {e}")
        return df

    @staticmethod
    def normalize_block_data(blocks: List) -> pl.DataFrame:
        """Normalize block data for Parquet conversion.

        Args:
            blocks: List of block objects from Web3

        Returns:
            pl.DataFrame: Normalized block data
        """
        normalized = []
        for block in blocks:
            try:
                # Convert AttributeDict to dict
                block_dict = dict(block) if hasattr(block, 'items') else block
                row = PickleToParquetConverter._convert_value(block_dict)

                # Convert Unix timestamp (seconds) to datetime
                timestamp_sec = block_dict.get('timestamp', 0)
                timestamp_dt = datetime.fromtimestamp(
                    timestamp_sec) if timestamp_sec else None

                row.update({
                    'number': block_dict.get('number'),
                    'hash': PickleToParquetConverter._convert_value(block_dict.get('hash', '')),
                    'parentHash': PickleToParquetConverter._convert_value(block_dict.get('parentHash', '')),
                    'miner': str(block_dict.get('miner', '')).lower() if block_dict.get('miner') else '',
                    'difficulty': block_dict.get('difficulty', 0),
                    'totalDifficulty': block_dict.get('totalDifficulty', 0),
                    'size': block_dict.get('size', 0),
                    'gasLimit': block_dict.get('gasLimit', 0),
                    'gasUsed': block_dict.get('gasUsed', 0),
                    'timestamp': timestamp_dt,
                    'transactions': [PickleToParquetConverter._convert_value(tx) for tx in block_dict.get('transactions', [])],
                    'uncles': [PickleToParquetConverter._convert_value(uncle) for uncle in block_dict.get('uncles', [])],
                })
                normalized.append(row)
            except Exception as e:
                logger.warning(
                    f"Error normalizing block {block_dict.get('number') if isinstance(block_dict, dict) else 'unknown'}: {e}")
                continue

        if not normalized:
            return pl.DataFrame(schema=PickleToParquetConverter.BLOCK_SCHEMA)
        return PickleToParquetConverter._dataframe_with_known_types(
            normalized, PickleToParquetConverter.BLOCK_SCHEMA)

    @staticmethod
    def normalize_receipt_data(receipts: List) -> pl.DataFrame:
        """Normalize receipt data for Parquet conversion.

        Args:
            receipts: List of receipt objects from Web3 (can be list of lists)

        Returns:
            pl.DataFrame: Normalized receipt data
        """
        # Flatten nested lists if necessary
        flat_receipts = []
        for receipt in receipts:
            if isinstance(receipt, list):
                flat_receipts.extend(receipt)
            else:
                flat_receipts.append(receipt)

        normalized = []
        for receipt in flat_receipts:
            try:
                # Convert AttributeDict to dict
                receipt_dict = dict(receipt) if hasattr(
                    receipt, 'items') else receipt
                row = PickleToParquetConverter._convert_value(receipt_dict)

                # Handle logs - can be list of AttributeDicts
                logs = []
                for log in receipt_dict.get('logs', []):
                    log_dict = dict(log) if hasattr(log, 'items') else log
                    log_row = PickleToParquetConverter._convert_value(log_dict)
                    log_row.update({
                        'address': str(log_dict.get('address', '')).lower() if log_dict.get('address') else '',
                        'topics': [str(PickleToParquetConverter._convert_value(t)) for t in log_dict.get('topics', [])],
                        'data': PickleToParquetConverter._convert_value(log_dict.get('data', '')),
                        # 'blockNumber': log_dict.get('blockNumber'),
                        # 'blockHash': PickleToParquetConverter._convert_value(log_dict.get('blockHash', '')),
                        # 'transactionHash': PickleToParquetConverter._convert_value(log_dict.get('transactionHash', '')),
                        # 'transactionIndex': log_dict.get('transactionIndex', 0),
                        'logIndex': log_dict.get('logIndex', 0),
                        'removed': log_dict.get('removed', False),
                    })
                    logs.append(log_row)

                row.update({
                    'blockNumber': receipt_dict.get('blockNumber'),
                    'transactionHash': PickleToParquetConverter._convert_value(receipt_dict.get('transactionHash', '')),
                    'transactionIndex': receipt_dict.get('transactionIndex', 0),
                    'blockHash': PickleToParquetConverter._convert_value(receipt_dict.get('blockHash', '')),
                    'from': str(receipt_dict.get('from', '')).lower() if receipt_dict.get('from') else '',
                    'to': str(receipt_dict.get('to', '')).lower() if receipt_dict.get('to') else '',
                    'cumulativeGasUsed': receipt_dict.get('cumulativeGasUsed', 0),
                    'gasUsed': receipt_dict.get('gasUsed', 0),
                    'contractAddress': str(receipt_dict.get('contractAddress', '')).lower() if receipt_dict.get('contractAddress') else '',
                    'logs': logs,
                    'status': receipt_dict.get('status', False),
                    'timeboosted': receipt_dict.get('timeboosted', False),
                })
                normalized.append(row)
            except Exception as e:
                logger.warning(f"Error normalizing receipt: {e}")
                continue

        if not normalized:
            return pl.DataFrame(schema=PickleToParquetConverter.RECEIPT_SCHEMA)
        return PickleToParquetConverter._dataframe_with_known_types(
            normalized, PickleToParquetConverter.RECEIPT_SCHEMA)

    @staticmethod
    def normalize_transaction_data(transactions: List) -> pl.DataFrame:
        """Normalize transaction data for Parquet conversion.

        Args:
            transactions: List of transaction objects from Web3 (can be list of lists)

        Returns:
            pl.DataFrame: Normalized transaction data
        """
        # Flatten nested lists if necessary
        flat_transactions = []
        for transaction in transactions:
            if isinstance(transaction, list):
                flat_transactions.extend(transaction)
            else:
                flat_transactions.append(transaction)

        normalized = []
        for transaction in flat_transactions:
            try:
                # Convert AttributeDict to dict
                transaction_dict = dict(transaction) if hasattr(
                    transaction, 'items') else transaction
                row = PickleToParquetConverter._convert_value(transaction_dict)

                # Handle logs - can be list of AttributeDicts
                # logs = []
                # for log in transaction_dict.get('logs', []):
                #     log_dict = dict(log) if hasattr(log, 'items') else log
                #     logs.append({
                #         'address': str(log_dict.get('address', '')).lower() if log_dict.get('address') else '',
                #         'topics': [str(PickleToParquetConverter._convert_value(t)) for t in log_dict.get('topics', [])],
                #         'data': PickleToParquetConverter._convert_value(log_dict.get('data', '')),
                #         # 'blockNumber': log_dict.get('blockNumber'),
                #         # 'blockHash': PickleToParquetConverter._convert_value(log_dict.get('blockHash', '')),
                #         # 'transactionHash': PickleToParquetConverter._convert_value(log_dict.get('transactionHash', '')),
                #         # 'transactionIndex': log_dict.get('transactionIndex', 0),
                #         'logIndex': log_dict.get('logIndex', 0),
                #         'removed': log_dict.get('removed', False),
                #     })

                row.update({
                    'blockNumber': transaction_dict.get('blockNumber'),
                    'transactionHash': PickleToParquetConverter._convert_value(transaction_dict.get('transactionHash', '')),
                    'transactionIndex': transaction_dict.get('transactionIndex', 0),
                    'blockHash': PickleToParquetConverter._convert_value(transaction_dict.get('blockHash', '')),
                    'from': str(transaction_dict.get('from', '')).lower() if transaction_dict.get('from') else '',
                    'to': str(transaction_dict.get('to', '')).lower() if transaction_dict.get('to') else '',
                })
                normalized.append(row)
            except Exception as e:
                logger.warning(f"Error normalizing transaction: {e}")
                continue

        if not normalized:
            return pl.DataFrame(schema=PickleToParquetConverter.TRANSACTION_SCHEMA)
        return PickleToParquetConverter._dataframe_with_known_types(
            normalized, PickleToParquetConverter.TRANSACTION_SCHEMA)

    def save_parquet(self, df: pl.DataFrame, output_path: Union[str, Path],
                     compression: str = 'zstd') -> None:
        """Save DataFrame to Parquet file with optimized settings.

        Args:
            df: DataFrame to save
            output_path: Output file path
            compression: Compression codec ('snappy', 'gzip', 'brotli', 'zstd', 'none')
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        parquet_compression = 'uncompressed' if compression == 'none' else compression

        try:
            df.write_parquet(
                output_path,
                compression=parquet_compression,
            )
            logger.info(f"✓ Saved: {output_path} ({len(df)} rows)")
        except Exception as e:
            logger.error(f"Error saving {output_path}: {e}")
            raise

    def convert_blocks(self, input_dir: Union[str, Path],
                       output_dir: Optional[Union[str, Path]] = None,
                       pattern: str = "blocks_*.pkl.gz",
                       compression: str = 'zstd',
                       max_workers: Optional[int] = None) -> Tuple[int, int]:
        """Convert block pickle files to Parquet.

        Args:
            input_dir: Directory containing block pickle files
            output_dir: Directory to save Parquet files
            pattern: Glob pattern for pickle files
            compression: Compression codec

        Returns:
            Tuple[int, int]: (successful_conversions, failed_conversions)
        """
        input_dir = Path(input_dir)
        output_dir = Path(output_dir or self.output_dir / "blocks")

        pickle_files = sorted(input_dir.glob(pattern))
        if not pickle_files:
            logger.warning(
                f"No files matching pattern '{pattern}' in {input_dir}")
            return 0, 0

        logger.info(f"Found {len(pickle_files)} block pickle files")
        successful, failed = 0, 0

        # Parallel processing of files
        # determine max_workers (limit to 8 by default)
        if max_workers is None:
            max_workers = min(
                max(1, (os.cpu_count() or 1) - 1), len(pickle_files), 8)
        else:
            max_workers = max(1, min(max_workers, len(pickle_files)))

        logger.debug(
            f"Using max_workers={max_workers}; limited internal threads")
        futures = []
        with ProcessPoolExecutor(max_workers=max_workers) as exe:
            for pickle_file in pickle_files:
                futures.append(exe.submit(_process_block_file, str(
                    pickle_file), str(output_dir), compression))

            for fut in tqdm(as_completed(futures), total=len(futures), desc="Converting blocks"):
                try:
                    name, ok, err, duration = fut.result()
                    if ok:
                        successful += 1
                        logger.info(f"{name} done in {duration:.1f}s")
                    else:
                        failed += 1
                        logger.warning(f"{name}: {err} (in {duration:.1f}s)")
                except Exception as e:
                    failed += 1
                    logger.error(f"Worker failed: {e}")

        logger.info(
            f"Block conversion complete: {successful} successful, {failed} failed")
        return successful, failed

    def convert_receipts(self, input_dir: Union[str, Path],
                         output_dir: Optional[Union[str, Path]] = None,
                         pattern: str = "blocks_receipts_*.pkl.gz",
                         compression: str = 'zstd',
                         max_workers: Optional[int] = None) -> Tuple[int, int]:
        """Convert receipt pickle files to Parquet.

        Args:
            input_dir: Directory containing receipt pickle files
            output_dir: Directory to save Parquet files
            pattern: Glob pattern for pickle files
            compression: Compression codec

        Returns:
            Tuple[int, int]: (successful_conversions, failed_conversions)
        """
        input_dir = Path(input_dir)
        output_dir = Path(output_dir or self.output_dir / "blocks_receipts")

        pickle_files = sorted(input_dir.glob(pattern))
        if not pickle_files:
            logger.warning(
                f"No files matching pattern '{pattern}' in {input_dir}")
            return 0, 0

        logger.info(f"Found {len(pickle_files)} receipt pickle files")
        successful, failed = 0, 0

        # Parallel processing of files
        if max_workers is None:
            max_workers = min(
                max(1, (os.cpu_count() or 1) - 1), len(pickle_files), 8)
        else:
            max_workers = max(1, min(max_workers, len(pickle_files)))

        logger.debug(
            f"Using max_workers={max_workers}; limited internal threads")
        futures = []
        with ProcessPoolExecutor(max_workers=max_workers) as exe:
            for pickle_file in pickle_files:
                futures.append(exe.submit(_process_receipt_file, str(
                    pickle_file), str(output_dir), compression))

            for fut in tqdm(as_completed(futures), total=len(futures), desc="Converting receipts"):
                try:
                    name, ok, err, duration = fut.result()
                    if ok:
                        successful += 1
                        logger.info(f"{name} done in {duration:.1f}s")
                    else:
                        failed += 1
                        logger.warning(f"{name}: {err} (in {duration:.1f}s)")
                except Exception as e:
                    failed += 1
                    logger.error(f"Worker failed: {e}")

        logger.info(
            f"Receipt conversion complete: {successful} successful, {failed} failed")
        return successful, failed

    def convert_transactions(self, input_dir: Union[str, Path],
                             output_dir: Optional[Union[str, Path]] = None,
                             pattern: str = "transactions_*.pkl.gz",
                             compression: str = 'zstd',
                             max_workers: Optional[int] = None) -> Tuple[int, int]:
        """Convert transaction pickle files to Parquet.

        Args:
            input_dir: Directory containing transaction pickle files
            output_dir: Directory to save Parquet files
            pattern: Glob pattern for pickle files
            compression: Compression codec

        Returns:
            Tuple[int, int]: (successful_conversions, failed_conversions)
        """
        input_dir = Path(input_dir)
        output_dir = Path(output_dir or self.output_dir / "transactions")

        pickle_files = sorted(input_dir.glob(pattern))
        if not pickle_files:
            logger.warning(
                f"No files matching pattern '{pattern}' in {input_dir}")
            return 0, 0

        logger.info(f"Found {len(pickle_files)} transaction pickle files")
        successful, failed = 0, 0

        # Parallel processing of files
        if max_workers is None:
            max_workers = min(
                max(1, (os.cpu_count() or 1) - 1), len(pickle_files), 8)
        else:
            max_workers = max(1, min(max_workers, len(pickle_files)))

        logger.debug(
            f"Using max_workers={max_workers}; limited internal threads")
        futures = []
        with ProcessPoolExecutor(max_workers=max_workers) as exe:
            for pickle_file in pickle_files:
                futures.append(exe.submit(_process_transaction_file, str(
                    pickle_file), str(output_dir), compression))

            for fut in tqdm(as_completed(futures), total=len(futures), desc="Converting transactions"):
                try:
                    name, ok, err, duration = fut.result()
                    if ok:
                        successful += 1
                        logger.info(f"{name} done in {duration:.1f}s")
                    else:
                        failed += 1
                        logger.warning(f"{name}: {err} (in {duration:.1f}s)")
                except Exception as e:
                    failed += 1
                    logger.error(f"Worker failed: {e}")

        logger.info(
            f"Transaction conversion complete: {successful} successful, {failed} failed")
        return successful, failed

    def convert_all(self, blocks_dir: Union[str, Path],
                    receipts_dir: Union[str, Path],
                    output_dir: Optional[Union[str, Path]] = None,
                    max_workers: Optional[int] = None) -> Dict[str, Tuple[int, int]]:
        """Convert all block and receipt files.

        Args:
            blocks_dir: Directory with block pickle files
            receipts_dir: Directory with receipt pickle files
            output_dir: Output directory for Parquet files

        Returns:
            Dict with conversion statistics
        """
        output_dir = Path(output_dir or self.output_dir)
        start_time = datetime.now()

        logger.info("="*60)
        logger.info("Starting Pickle to Parquet Conversion")
        logger.info("="*60)

        # Convert blocks
        logger.info("\nProcessing blocks...")
        blocks_success, blocks_failed = self.convert_blocks(
            blocks_dir, output_dir / "blocks", max_workers=max_workers
        )

        # Convert receipts
        logger.info("\nProcessing receipts...")
        receipts_success, receipts_failed = self.convert_receipts(
            receipts_dir, output_dir / "blocks_receipts", max_workers=max_workers
        )

        # Summary
        elapsed = datetime.now() - start_time
        total_success = blocks_success + receipts_success
        total_failed = blocks_failed + receipts_failed

        summary = {
            'blocks': (blocks_success, blocks_failed),
            'receipts': (receipts_success, receipts_failed),
            'total_success': total_success,
            'total_failed': total_failed,
            'elapsed_seconds': elapsed.total_seconds(),
        }

        logger.info("\n" + "="*60)
        logger.info("Conversion Summary")
        logger.info("="*60)
        logger.info(f"Blocks:   {blocks_success} ✓, {blocks_failed} ✗")
        logger.info(f"Receipts: {receipts_success} ✓, {receipts_failed} ✗")
        logger.info(f"Total:    {total_success} ✓, {total_failed} ✗")
        logger.info(f"Time:     {elapsed.total_seconds():.1f}s")
        logger.info(f"Output:   {output_dir}")
        logger.info("="*60)

        return summary


def convert_pickle_to_parquet(blocks_dir: Union[str, Path],
                              receipts_dir: Union[str, Path],
                              output_dir: Optional[Union[str, Path]] = None,
                              compression: str = 'zstd') -> None:
    """Convenience function to convert all pickle files to Parquet.

    Args:
        blocks_dir: Directory with block pickle files
        receipts_dir: Directory with receipt pickle files
        output_dir: Output directory (defaults to ./parquet_output)
        compression: Compression codec (default: 'zstd')
    """
    converter = PickleToParquetConverter(output_dir=output_dir)
    converter.convert_all(blocks_dir, receipts_dir, output_dir)


if __name__ == "__main__":
    # Example usage
    import argparse

    parser = argparse.ArgumentParser(
        description="Convert blockchain pickle files to Parquet format"
    )
    parser.add_argument(
        "--blocks-dir",
        type=str,
        required=True,
        help="Directory containing block pickle files"
    )
    parser.add_argument(
        "--receipts-dir",
        type=str,
        required=True,
        help="Directory containing receipt pickle files"
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
        choices=["snappy", "gzip", "brotli", "none", "zstd"],
        help="Compression codec (default: zstd)"
    )

    args = parser.parse_args()

    convert_pickle_to_parquet(
        blocks_dir=args.blocks_dir,
        receipts_dir=args.receipts_dir,
        output_dir=args.output_dir,
        compression=args.compression
    )


def _process_block_file(pickle_file: str, output_dir: str, compression: str) -> tuple:
    """Worker for processing a single block pickle file.

    Returns: (filename, True/False, error_message_or_none)
    """
    start = datetime.now()
    try:
        conv = PickleToParquetConverter(output_dir=output_dir)
        blocks = conv.load_pickle_file(pickle_file)
        if not blocks:
            return (os.path.basename(pickle_file), False, "empty file", 0.0)
        df = conv.normalize_block_data(blocks)
        if df.height == 0:
            return (os.path.basename(pickle_file), False, "no valid data", 0.0)
        output_name = Path(pickle_file).stem.replace('.pkl', '') + '.parquet'
        output_path = Path(output_dir) / output_name
        conv.save_parquet(df, output_path, compression=compression)
        duration = (datetime.now() - start).total_seconds()
        return (os.path.basename(pickle_file), True, None, duration)
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        return (os.path.basename(pickle_file), False, str(e), duration)


def _process_receipt_file(pickle_file: str, output_dir: str, compression: str) -> tuple:
    """Worker for processing a single receipt pickle file.

    Returns: (filename, True/False, error_message_or_none)
    """
    start = datetime.now()
    try:
        conv = PickleToParquetConverter(output_dir=output_dir)
        receipts = conv.load_pickle_file(pickle_file)
        if not receipts:
            return (os.path.basename(pickle_file), False, "empty file", 0.0)
        df = conv.normalize_receipt_data(receipts)
        if df.height == 0:
            return (os.path.basename(pickle_file), False, "no valid data", 0.0)
        output_name = Path(pickle_file).stem.replace('.pkl', '') + '.parquet'
        output_path = Path(output_dir) / output_name
        conv.save_parquet(df, output_path, compression=compression)
        duration = (datetime.now() - start).total_seconds()
        return (os.path.basename(pickle_file), True, None, duration)
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        return (os.path.basename(pickle_file), False, str(e), duration)


def _process_transaction_file(pickle_file: str, output_dir: str, compression: str) -> tuple:
    """Worker for processing a single transaction pickle file.

    Returns: (filename, True/False, error_message_or_none)
    """
    start = datetime.now()
    try:
        conv = PickleToParquetConverter(output_dir=output_dir)
        transactions = conv.load_pickle_file(pickle_file)
        if not transactions:
            return (os.path.basename(pickle_file), False, "empty file", 0.0)
        df = conv.normalize_transaction_data(transactions)
        if df.height == 0:
            return (os.path.basename(pickle_file), False, "no valid data", 0.0)
        output_name = Path(pickle_file).stem.replace('.pkl', '') + '.parquet'
        output_path = Path(output_dir) / output_name
        conv.save_parquet(df, output_path, compression=compression)
        duration = (datetime.now() - start).total_seconds()
        return (os.path.basename(pickle_file), True, None, duration)
    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        return (os.path.basename(pickle_file), False, str(e), duration)
