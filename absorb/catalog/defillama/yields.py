from __future__ import annotations

import typing

import absorb
from . import common


if typing.TYPE_CHECKING:
    import polars as pl


class Yields(absorb.Table):
    source = 'defillama'
    description = 'Yields of each pool in USD over time'
    url = 'https://defillama.com/'
    write_range = 'overwrite_all'
    parameter_types = {'pools': (list, type(None)), 'top_n': int}
    default_parameters = {'pools': None, 'top_n': 5000}
    row_precision = 'day'
    name_template = [
        'pool_yields_top_{top_n}',
        'pool_yields_{pools}',
        'pool_yields',
    ]

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'pool': pl.String,
            'chain': pl.String,
            'project': pl.String,
            'symbol': pl.String,
            'tvl_usd': pl.Float64,
            'apy_base': pl.Float64,
            'apy_base_7d': pl.Float64,
            'apy_reward': pl.Float64,
            'il_7d': pl.Float64,
            'revenue': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import time
        import polars as pl

        # get current yield pools
        current_yields = get_current_yields()

        # select which pools to collect
        pools = self.parameters['pools']
        if pools is None:
            pools = current_yields.sort('tvl_usd', descending=True)['pool']
            if self.parameters['top_n'] is not None:
                pools = pools[: self.parameters['top_n']]

        # collect yields for each pool
        next_time = time.time()
        dfs = []
        print('collecting', len(pools), 'pools')
        for p, pool in enumerate(pools, start=1):
            while time.time() < next_time:
                time.sleep(0.05)
            print('[' + str(p) + ' / ' + str(len(pools)) + ']', pool)
            try:
                df = get_historical_yields_of_pool(pool)
                dfs.append(df)
            except Exception:
                pass
            next_time = next_time + 4.0
        df = pl.concat(dfs)

        # get labels
        current_yields = current_yields[['pool', 'chain', 'project', 'symbol']]

        return df.join(current_yields, on='pool', how='left').select(
            self.get_schema().keys()
        )


def get_current_yields() -> pl.DataFrame:
    import polars as pl

    data = common._fetch('current_yields')

    columns = {
        'pool': 'pool',
        'chain': 'chain',
        'project': 'project',
        'symbol': 'symbol',
        'tvl_usd': 'tvlUsd',
        'apy_base': 'apyBase',
        'apy_reward': 'apyReward',
        'apy': 'apy',
        'apy_pct_1D': 'apyPct1D',
        'apy_pct_7D': 'apyPct7D',
        'apy_pct_30D': 'apyPct30D',
        'apy_mean_30d': 'apyMean30d',
        'apy_base_7d': 'apyBase7d',
        'apy_base_inception': 'apyBaseInception',
        'volume_usd_1d': 'volumeUsd1d',
        'volume_usd_7d': 'volumeUsd7d',
        'reward_tokens': 'rewardTokens',
        'underlying_tokens': 'underlyingTokens',
        'stablecoin': 'stablecoin',
        'il_risk': 'ilRisk',
        'il_7d': 'il7d',
        'exposure': 'exposure',
        'pool_meta': 'poolMeta',
        'mu': 'mu',
        'sigma': 'sigma',
        'count': 'count',
        'outlier': 'outlier',
    }

    return (
        pl.DataFrame(data['data'], infer_schema_length=99999999)
        .select(**columns)
        .sort('tvl_usd', descending=True)
    )


def get_historical_yields_of_pool(pool: str) -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_yields_per_pool', {'pool': pool})
    columns: dict[str, str | pl.Expr] = {
        'timestamp': pl.col.timestamp.str.to_datetime()
        .dt.replace_time_zone('UTC')
        .dt.cast_time_unit('us'),
        'pool': pl.lit(pool, dtype=pl.String),
        'tvl_usd': 'tvlUsd',
        'apy_base': 'apyBase',
        'apy_base_7d': 'apyBase7d',
        'apy_reward': 'apyReward',
        'il_7d': 'il7d',
    }
    raw_schema = {
        'timestamp': pl.String,
        'tvlUsd': pl.Float64,
        'apyBase': pl.Float64,
        'apyBase7d': pl.Float64,
        'apyReward': pl.Float64,
        'il7d': pl.Float64,
    }
    return (
        pl.DataFrame(data['data'], schema=raw_schema)
        .select(**columns)
        .with_columns(
            revenue=pl.col.tvl_usd * (pl.col.apy_base + pl.col.apy_reward),
        )
    )
