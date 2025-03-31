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
3. Supported Datasets
4. Filesystem Layout


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


## Supported Datasets

`truck` collects each of these datasets as a collection of parquet files:

- [4byte](https://www.4byte.directory) function and event signatures
- [chain_ids](https://github.com/ethereum-lists/chains) chain id's
- [cryo](https://github.com/paradigmxyz/cryo) EVM datasets
- [dune](https://dune.com) tables and queries
- [growthepie](https://www.growthepie.xyz/) L2 metrics
- [kalshi](https://kalshi.com) daily market summaries
- [mempool dumpster](https://mempool-dumpster.flashbots.net) mempool history
- [sourcify](https://sourcify.dev) verified contracts
- [tix](https://github.com/paradigmxyz/tix) price feeds
- [xatu](https://github.com/ethpandaops/xatu-data) mempool datasets


## Filesystem Layout

truck stores all datasets as parquet files

datasets can be stored in any location on your disks, and truck will use symlinks to organize those files in the `TRUCK_ROOT` tree

the `TRUCK_ROOT` filesystem directory is organized as:

```
{TRUCK_ROOT}/
    datasets/
        <dataset>/
            tables/
                <datatype>/
                    {filename}.parquet
            repos/
                {repo_name}/
            dataset_metadata.json
    truck_config.json
```

schema of `truck_config.json`:

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
