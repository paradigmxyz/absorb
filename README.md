# truck 🚛

simple interface for collecting, managing, and loading datasets

## Features
- coverage
    - access > 1 million public datasets
    - integrate new datasets with minimal code
- collection
    - collect any dataset with a single cli command
    - maintain dataset freshness as they update over time
- loading
    - load any dataset as polars dataframe with a single function call
- customization
    - define custom datasets, either from scratch, or as views of other datasets
    - share custom datasets by uploading with single cli commnand
    - define custom cli for each dataset


## Contents
1. Installation
2. Example Usage
    i. Command Line
    ii. Python
3. Supported Data sources
4. Output Format
5. Configuration


## Installation
`uv install truck`


## Example Usage

#### Example Command Line Usage

```bash
# collect dataset and save as local files
truck collect kalshi

# list datasets that are collected or available
truck ls

# show schemas of dataset
truck schema kalshi

# create new custom dataset
truck new custom_dataset

# upload custom dataset
truck upload custom_dataset
```

#### Example Python Usage

```python
import truck

# collect dataset and save as local files
truck.collect('kalshi')

# list datasets that are collected or available
datasets = truck.list()

# get schemas of dataset
schema = truck.schema('kalshi')

# load dataset as polars DataFrame
df = truck.load('kalshi')

# scan dataset as polars LazyFrame
lf = truck.scan('kalshi')

# create new custom dataset
truck.new('custom_dataset')

# upload custom dataset
truck.upload('custom_dataset')
```


## Supported Data Sources

`truck` collects data from each of these sources:

- [4byte](https://www.4byte.directory) function and event signatures
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
- [sourcify](https://sourcify.dev) verified contracts
- [tix](https://github.com/paradigmxyz/tix) price feeds
- [vera](https://verifieralliance.org) verified contract archives
- [xatu](https://github.com/ethpandaops/xatu-data) many Ethereum datasets

To list all available datasets and data sources, type `truck ls` on the command line.


## Output Format

To display information about the schema and other metadata of a dataset, type `truck help <DATASET>` on the command line.

`truck` stores each dataset as a collection of parquet files.

Datasets can be stored in any location on your disks, and truck will use symlinks to organize those files in the `TRUCK_ROOT` tree.

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
    truck_config.json
```

## Configuration

`truck` uses a config file to specify which datasets to track.

Schema of `truck_config.json`:

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
