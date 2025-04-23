from __future__ import annotations

import typing

import truck
from . import common


if typing.TYPE_CHECKING:
    import polars as pl


class YieldsOfPools(truck.Table):
    parameter_types = {'pools': list[str]}

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        dfs = []
        for pool in self.parameters['pools']:
            df = get_historical_yields_of_pool(pool)
            dfs.append(df)
        return pl.concat(dfs)


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
        'timestamp': pl.col.timestamp.str.to_datetime(),
        'pool': pl.lit(pool),
        'tvl_usd': 'tvlUsd',
        'apy_base': 'apyBase',
        'apy_base_7d': 'apyBase7d',
        'apy_reward': 'apyReward',
        'il_7d': 'il7d',
    }
    return pl.DataFrame(data['data'], infer_schema_length=9999999999).select(
        **columns
    )
