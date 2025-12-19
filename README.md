# Node Crawler (Simplified)

Lightweight tools for crawling Ethereum blocks/receipts and converting the
resulting pickle files into Parquet for analysis.

## Author

Johnnatan Messias  
https://johnnatan-messias.github.io  
https://www.linkedin.com/in/johnnatan-messias/  
https://twitter.com/johnnatan_me  
https://scholar.google.com/citations?user=EoGEeFAAAAAJ

## Citation

If this repository was useful, please cite it.

## Repository layout

```
.
├── README.md
├── __init__.py
├── requirements.txt
├── crawler.py
├── convert_pickle_to_parquet.py
└── pickle_to_parquet.py
```

## Requirements

- Python 3.10+ (recommended)
- An Ethereum JSON-RPC endpoint (archive node recommended)

Install dependencies:

```
pip install -r requirements.txt
```

## Usage

### Crawl blocks

```
python crawler.py \
  --method blocks \
  --min 290688000 \
  --max 290688100 \
  --batch-size 1000 \
  --max-workers 20 \
  --node-endpoint http://ethereum-archive:8545 \
  --timeout 60
```

### Crawl block receipts

```
python crawler.py \
  --method block_receipts \
  --min 290688000 \
  --max 290688100 \
  --batch-size 1000 \
  --max-workers 20 \
  --node-endpoint http://ethereum-archive:8545 \
  --timeout 60
```

Crawler output is written to `data/blocks` and `data/blocks_receipts` under the
repo root.

### Convert pickle to Parquet

```
python convert_pickle_to_parquet.py \
  --blocks-dir ./data/blocks \
  --receipts-dir ./data/blocks_receipts \
  --output-dir ./parquet_output \
  --compression zstd
```

Optional flags:

- `--workers` to control parallelism
- `--blocks-only` or `--receipts-only` to limit what is converted
- `-v` for verbose logging

## Notes

- The crawler creates `data/` directories automatically.
- Parquet output defaults to `./parquet_output` if `--output-dir` is omitted.
