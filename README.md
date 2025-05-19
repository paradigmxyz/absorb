![image](https://github.com/user-attachments/assets/c53bfa3f-aa81-4a5a-a3f4-e9ad3b500359)

# absorb 🧽

*the sovereign dataset manager*

`absorb` makes it easy to 1) collect, 2) query, 3) manage, and 4) customize datasets from nearly any data source

## features
- **limitless dataset library**: access to millions of datasets across 16 diverse data sources
- **intuitive cli+python interfaces**: collect or query any dataset in a single line of code
- **maximal modularity**: built on open standards for frictionless integration with other tools
- **easy extensibility**: add new datasets or data sources with just a few lines of code

## Contents
1. Installation
2. Example Usage
    i. Command Line
    ii. Python
3. Supported Data sources
4. Output Format
5. Configuration


## Installation
`uv install absorb`


## Example Usage

#### Example Command Line Usage

```bash
# collect dataset and save as local files
absorb collect kalshi

# list datasets that are collected or available
absorb ls

# show schemas of dataset
absorb schema kalshi

# create new custom dataset
absorb new custom_dataset

# upload custom dataset
absorb upload custom_dataset
```

#### Example Python Usage

```python
import absorb

# collect dataset and save as local files
absorb.collect('kalshi')

# list datasets that are collected or available
datasets = absorb.list()

# get schemas of dataset
schema = absorb.schema('kalshi')

# load dataset as polars DataFrame
df = absorb.load('kalshi')

# scan dataset as polars LazyFrame
lf = absorb.scan('kalshi')

# create new custom dataset
absorb.new('custom_dataset')

# upload custom dataset
absorb.upload('custom_dataset')
```


## Supported Data Sources

`absorb` collects data from each of these sources:

- [4byte](https://www.4byte.directory) function and event signatures
- [allium](https://www.allium.so) crypto data platform
- [bigquery](https://cloud.google.com/blockchain-analytics/docs/supported-datasets) crypto ETL datasets
- [binance](https://data.binance.vision) trades and OHLC candles on the Binance CEX
- [blocknative](https://docs.blocknative.com/data-archive/mempool-archive) Ethereum mempool archive
- [chain_ids](https://github.com/ethereum-lists/chains) chain id's
- [coingecko](https://www.coingecko.com/) token prices
- [cryo](https://github.com/paradigmxyz/cryo) EVM datasets
- [defillama](https://defillama.com) DeFi data
- [dune](https://dune.com) tables and queries
- [flipside](https://flipsidecrypto.xyz) crypto data platform
- [growthepie](https://www.growthepie.xyz) L2 metrics
- [kalshi](https://kalshi.com) prediction market metrics
- [l2beat](https://l2beat.com) L2 metrics
- [mempool dumpster](https://mempool-dumpster.flashbots.net) Ethereum mempool archive
- [snowflake](https://www.snowflake.com/) generalized data platform
- [sourcify](https://sourcify.dev) verified contracts
- [tix](https://github.com/paradigmxyz/tix) price feeds
- [vera](https://verifieralliance.org) verified contract archives
- [xatu](https://github.com/ethpandaops/xatu-data) many Ethereum datasets

To list all available datasets and data sources, type `absorb ls` on the command line.


## Output Format

To display information about the schema and other metadata of a dataset, type `absorb help <DATASET>` on the command line.

`absorb` stores each dataset as a collection of parquet files.

Datasets can be stored in any location on your disks, and absorb will use symlinks to organize those files in the `TRUCK_ROOT` tree.

the `TRUCK_ROOT` filesystem directory is organized as:

```
{TRUCK_ROOT}/
    datasets/
        <source>/
            tables/
                <datatype>/
                    {filename}.parquet
                table_metadata.json
            repos/
                {repo_name}/
    absorb_config.json
```

## Configuration

`absorb` uses a config file to specify which datasets to track.

Schema of `absorb_config.json`:

```python
{
    'tracked_tables': list[TrackedTable]
}
```

schema of `dataset_config.json`:

```python
{
    "name": str,
    "definition": str,
    "parameters": dict[str, Any],
    "repos": [str]
}
```
