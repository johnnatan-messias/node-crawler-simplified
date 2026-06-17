# Node Crawler Simplified

Lightweight Python tools for crawling Ethereum-compatible block data from a
JSON-RPC endpoint and saving it as Parquet for analysis. The repository also
includes utilities for converting older gzipped pickle outputs to Parquet.

## Author

Johnnatan Messias  
https://johnnatan-messias.github.io  
https://www.linkedin.com/in/johnnatan-messias/  
https://twitter.com/johnnatan_me  
https://scholar.google.com/citations?user=EoGEeFAAAAAJ

## Citation

If this repository is useful in your research or project, please cite it.

## Repository Layout

```text
.
├── crawler.py                  # Async JSON-RPC crawler for blocks and receipts
├── pickle_to_parquet.py        # Normalization and Parquet conversion helpers
├── convert_pickle_to_parquet.py # CLI for converting legacy pickle outputs
├── simple_pickle_to_parquet.py # Minimal generic pickle-to-Parquet converter
├── requirements.txt
├── LICENSE
└── README.md
```

Generated data is typically written under:

```text
data/
├── blocks/
└── blocks_receipts/
```

## Requirements

- Python 3.10+
- An Ethereum-compatible JSON-RPC endpoint
- An archive node is recommended for historical ranges

Install dependencies:

```bash
pip install -r requirements.txt
```

## Crawl Data

The crawler supports two methods:

- `blocks`: fetch block headers and transaction hashes by default
- `block_receipts`: fetch transaction receipts for each block

Block ranges use `--min` as inclusive and `--max` as exclusive. For example,
`--min 80000000 --max 80000010` crawls blocks `80000000` through `80000009`.

### Crawl Blocks

```bash
python crawler.py \
  --method blocks \
  --min 80000000 \
  --max 80000010 \
  --batch-size 1000 \
  --max-workers 20 \
  --node-endpoint http://ethereum-archive:8545 \
  --timeout 60 \
  --datadir data \
  --compression zstd
```

To include full transaction objects in block responses, add:

```bash
--full-transactions
```

### Crawl Block Receipts

```bash
python crawler.py \
  --method block_receipts \
  --min 80000000 \
  --max 80000010 \
  --batch-size 1000 \
  --max-workers 20 \
  --node-endpoint http://ethereum-archive:8545 \
  --timeout 60 \
  --datadir data \
  --compression zstd
```

### Polygon / POA Chains

For Polygon or another proof-of-authority-style chain that needs Web3's extra
data middleware, add:

```bash
--is-polygon
```

## Output

Crawler batches are saved directly as Parquet files:

```text
data/blocks/blocks_<start>_<end>.parquet
data/blocks_receipts/blocks_receipts_<start>_<end>.parquet
```

Existing Parquet batch files are skipped automatically, so interrupted crawls
can be restarted with the same arguments.

Supported compression codecs:

- `zstd` default
- `snappy`
- `gzip`
- `brotli`
- `none`

## Convert Legacy Pickle Files

Use `convert_pickle_to_parquet.py` to convert gzipped pickle files from older
crawler outputs. Inputs are optional unless you use one of the `--*-only` flags.

```bash
python convert_pickle_to_parquet.py \
  --blocks-dir ./data/blocks \
  --receipts-dir ./data/blocks_receipts \
  --output-dir ./parquet_output \
  --compression zstd
```

The converter writes each data type into a subdirectory:

```text
parquet_output/
├── blocks/
├── blocks_receipts/
└── transactions/
```

Optional flags:

- `--transactions-dir ./data/transactions` to convert transaction pickle files
- `--blocks-only`, `--receipts-only`, or `--transactions-only`
- `--workers N` to control parallel conversion workers
- `-v` or `--verbose` for debug logging

## Generic Pickle Conversion

For a simpler one-directory conversion that does not apply blockchain-specific
normalization, use:

```bash
python simple_pickle_to_parquet.py \
  --input-dir ./pickle \
  --output-dir ./parquet_output
```

## Notes

- The crawler creates output directories automatically.
- Use lower `--max-workers` values if your RPC endpoint rate limits requests.
- Very large ranges should be split into batches that fit your endpoint,
  memory, and downstream analysis workflow.
