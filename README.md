![image](https://github.com/user-attachments/assets/7323b83e-fc5b-496c-b67b-bad6a188873b)

# absorb 🧽🫧🫧

`absorb` makes it easy to 1) collect, 2) manage, 3) query, and 4) customize datasets from nearly any data source

🚧 ***this is a preview release of beta software, and it is still under active development*** 🚧

## Features
- **limitless dataset library**: access to millions of datasets across 20+ diverse data sources
- **intuitive cli+python interfaces**: collect or query any dataset in a single line of code
- **maximal modularity**: built on open standards for frictionless integration with other tools
- **easy extensibility**: add new datasets or data sources with just a few lines of code

## Contents
1. [Installation](#installation)
2. [Example Usage](#example-usage)
    1. [Command Line](#example-command-line-usage)
    2. [Python](#example-python-usage)
3. [Supported Data Sources](#supported-data-sources)
4. [Output Format](#output-format)
5. [Configuration](#configuration)


## Installation

basic installation
```bash
uv tool install paradigm_absorb
```

install with all extras
```bash
uv tool install paradigm_absorb[test,datasources,interactive]
```

install from source
```bash
git clone git@github.com:paradigmxyz/absorb.git
uv tool install --editable .[test,datasources,interactive]
```


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
absorb.collect('kalshi.metrics')

# get schemas of dataset
schema = absorb.get_schema('kalshi.metrics')

# query dataset eagerly, as polars DataFrame
df = absorb.query('kalshi.metrics')

# query dataset lazily, as polars LazyFrame
lf = absorb.query('kalshi.metrics', lazy=True)

# upload custom dataset
absorb.upload('source.table')
```


## Supported Data Sources

🚧 under construction 🚧

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
- [fred](https://fred.stlouisfed.org) federal macroeonomic data
- [git](https://git-scm.com) commits, authors, and file diffs of a repo
- [growthepie](https://www.growthepie.xyz) L2 metrics
- [kalshi](https://kalshi.com) prediction market metrics
- [l2beat](https://l2beat.com) L2 metrics
- [mempool dumpster](https://mempool-dumpster.flashbots.net) Ethereum mempool archive
- [snowflake](https://www.snowflake.com/) generalized data platform
- [sourcify](https://sourcify.dev) verified contracts
- [tic](https://ticdata.treasury.gov) usa treasury department data
- [tix](https://github.com/paradigmxyz/tix) price feeds
- [vera](https://verifieralliance.org) verified contract archives
- [xatu](https://github.com/ethpandaops/xatu-data) many Ethereum datasets

To list all available datasets and data sources, type `absorb ls` on the command line.

To display information about the schema and other metadata of a dataset, type `absorb help <DATASET>` on the command line.


## Output Format

`absorb` uses the filesystem as its database. Each dataset is stored as a collection of parquet files, either on local disk or in the cloud.

Datasets can be stored in any location on your disks, and absorb will use symlinks to organize those files in the `ABSORB_ROOT` tree.

the `ABSORB_ROOT` filesystem directory is organized as:

```
{ABSORB_ROOT}/
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
    'version': str,
    'tracked_tables': list[TableDict],
    'use_git': bool,
    'default_bucket': {
        'rclone_remote': str | None,
        'bucket_name': str | None,
        'path_prefix': str | None,
        'provider': str | None,
    },
}
```

schema of `dataset_config.json`:

```python
{
    'source_name': str,
    'table_name': str,
    'table_class': str,
    'parameters': dict[str, JSONValue],
    'table_version': str,
}
```
