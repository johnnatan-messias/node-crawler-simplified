# Blockchain crawler for fetching blocks and receipts
# Author: Johnnatan Messias
# https://johnnatan-messias.github.io
# https://www.linkedin.com/in/johnnatan-messias/
# https://twitter.com/johnnatan_me
# https://scholar.google.com/citations?user=EoGEeFAAAAAJ

import gzip
import pickle
import asyncio
import argparse
from pathlib import Path
from tqdm.asyncio import tqdm
from web3 import AsyncWeb3, AsyncHTTPProvider


async def check_connection():
    """Verify connection to the blockchain node and display connection info."""
    try:
        is_connected = await W3.is_connected()
        block_number = await W3.eth.block_number
        print(f"Is connected to node: {is_connected}")
        print(f"The most recent block is: {block_number}")
    except Exception as e:
        print(f"Warning: Failed to connect to node: {e}")


def get_batch_intervals(block_start, block_end, batch_size):
    """Generate batch intervals for block processing.

    Args:
        block_start: Starting block number (inclusive)
        block_end: Ending block number (exclusive)
        batch_size: Number of blocks per batch

    Returns:
        list: List of (start, end) tuples for each batch
    """
    return [
        (start, min(start + batch_size - 1, block_end - 1))
        for start in range(block_start, block_end, batch_size)
    ]


async def fetch_block(w3_async, block_number, full_transactions=False):
    """Fetch a single block without full transaction data."""
    return await w3_async.eth.get_block(block_number, full_transactions=full_transactions)


async def fetch_block_receipts(w3_async, block_number):
    """Fetch transaction receipts for a single block."""
    return await w3_async.eth.get_block_receipts(block_number)


async def get_blocks(w3_async, block_numbers, max_workers=20):
    """Fetch multiple blocks concurrently.

    Args:
        w3_async: Async Web3 instance
        block_numbers: List of block numbers to fetch
        max_workers: Maximum concurrent requests (default: 20)

    Returns:
        list: Block data for all blocks
    """
    semaphore = asyncio.Semaphore(max_workers)

    async def sem_task(bn):
        async with semaphore:
            return await fetch_block(w3_async, bn)

    tasks = [sem_task(bn) for bn in block_numbers]

    blocks = []
    for f in tqdm.as_completed(tasks, total=len(tasks), desc='Gathering blocks...', ascii=True):
        blocks.append(await f)

    return blocks


async def get_blocks_receipts(w3_async, block_numbers, max_workers=20):
    """Fetch receipts for multiple blocks concurrently.

    Args:
        w3_async: Async Web3 instance
        block_numbers: List of block numbers to fetch
        max_workers: Maximum concurrent requests (default: 20)

    Returns:
        list: Receipt data for all blocks
    """
    semaphore = asyncio.Semaphore(max_workers)

    async def sem_task(bn):
        async with semaphore:
            return await fetch_block_receipts(w3_async, bn)

    tasks = [sem_task(bn) for bn in block_numbers]

    blocks = []
    for f in tqdm.as_completed(tasks, total=len(tasks), desc='Gathering blocks receipts...', ascii=True):
        blocks.append(await f)

    return blocks


async def crawl_block_data(block_number_min, block_number_max, batch_size=100_000, max_workers=20):
    """Fetch and save blocks in batches.

    Args:
        block_number_min: Starting block number (inclusive)
        block_number_max: Ending block number (inclusive)
        batch_size: Blocks per batch (default: 100,000)
        max_workers: Concurrent requests (default: 20)
    """
    await check_connection()

    intervals = get_batch_intervals(
        block_number_min, block_number_max, batch_size=batch_size)

    for block_start, block_end in tqdm(intervals, desc="Fetching blocks", ascii=True):
        blocks = await get_blocks(
            block_numbers=range(block_start, block_end + 1),
            w3_async=W3,
            max_workers=max_workers
        )

        file_path = BLOCKS_DIR / f"blocks_{block_start}_{block_end}.pkl.gz"
        with gzip.open(file_path, 'wb') as f:
            pickle.dump(blocks, f)


async def crawl_block_receipts_data(block_number_min, block_number_max, batch_size=100_000, max_workers=20):
    """Fetch and save block receipts in batches.

    Args:
        block_number_min: Starting block number (inclusive)
        block_number_max: Ending block number (inclusive)
        batch_size: Blocks per batch (default: 100,000)
        max_workers: Concurrent requests (default: 20)
    """
    await check_connection()

    intervals = get_batch_intervals(
        block_number_min, block_number_max, batch_size=batch_size)

    for block_start, block_end in tqdm(intervals, desc="Fetching blocks receipts", ascii=True):
        blocks_receipts = await get_blocks_receipts(
            block_numbers=range(block_start, block_end + 1),
            w3_async=W3,
            max_workers=max_workers
        )

        file_path = BLOCKS_RECEIPTS_DIR / \
            f"blocks_receipts_{block_start}_{block_end}.pkl.gz"
        with gzip.open(file_path, 'wb') as f:
            pickle.dump(blocks_receipts, f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Crawl blockchain data for a given block range."
    )

    parser.add_argument(
        "--method",
        type=str,
        choices=["blocks", "block_receipts"],
        required=True,
        help="Select the method to run (1: block_receipts and 2: blocks)."
    )

    parser.add_argument(
        "--min",
        type=int,
        default=290_688_000,
        help="Minimum block number (inclusive). Default: 290,688,000"
    )

    parser.add_argument(
        "--max",
        type=int,
        default=405_863_595+1,  # 410_939_508 + 1
        help="Maximum block number (exclusive). Default: 405_863_596"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="Number of blocks to process per batch. Default: 100,000"
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=20,
        help="Maximum number of concurrent requests. Default: 20"
    )
    parser.add_argument(
        "--node-endpoint",
        type=str,
        default="http://ethereum-archive:8545",
        help="Define the code endpoint. Default: http://ethereum-archive:8545"
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Define the connection timeout in seconds. Default: 60 seconds"
    )

    parser.add_argument(
        "--datadir",
        type=str,
        default="./data",
        help="Define the data directory name. Default: ./data"
    )

    args = parser.parse_args()

    block_number_min = args.min
    block_number_max = args.max
    batch_size = args.batch_size
    max_workers = args.max_workers
    timeout = args.timeout
    node_endpoint = args.node_endpoint
    datadir = args.datadir

    print(f"Block range: {block_number_min} → {block_number_max}")
    print(f"Batch size: {batch_size}")
    print(f"Max workers (concurrency): {max_workers}")
    print(f"Data directory: {datadir}")
    print(f"Timeout: {timeout}")
    print(f"Node endpoint: {node_endpoint}")

    # Node connection
    W3 = AsyncWeb3(AsyncHTTPProvider(
        node_endpoint, request_kwargs={'timeout': timeout}))

    # Setup directories for data storage
    _BASE_DIR = Path(__file__).parent / datadir
    print(_BASE_DIR)

    BLOCKS_DIR = _BASE_DIR / "blocks"
    TXS_DIR = _BASE_DIR / "transactions"
    BLOCKS_RECEIPTS_DIR = _BASE_DIR / "blocks_receipts"

    BLOCKS_DIR.mkdir(parents=True, exist_ok=True)
    TXS_DIR.mkdir(parents=True, exist_ok=True)
    BLOCKS_RECEIPTS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        if args.method == "block_receipts":
            asyncio.run(crawl_block_receipts_data(
                block_number_min,
                block_number_max,
                batch_size=batch_size,
                max_workers=max_workers
            ))
        elif args.method == "blocks":
            asyncio.run(crawl_block_data(
                block_number_min,
                block_number_max,
                batch_size=batch_size,
                max_workers=max_workers
            ))
    except KeyboardInterrupt:
        print("Interrupted by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
