from __future__ import annotations

import typing

from . import common

if typing.TYPE_CHECKING:
    import polars as pl


def get_project_metadata() -> pl.DataFrame:
    import polars as pl

    data = common._fetch('current_tvls')

    return pl.DataFrame(
        data, orient='row', infer_schema_length=len(data), strict=False
    ).select(
        'name',
        'slug',
        pl.col.parentProtocol.str.strip_prefix('parent#').alias('parent'),
        'category',
        (pl.col.listedAt * 1000).cast(pl.Datetime('ms')).alias('list_date'),
        'symbol',
        'chain',
        'chains',
        'url',
        'github',
        pl.col.tvl.alias('tvl_usd'),
    )


def get_historical_tvl() -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_tvl')
    return pl.DataFrame(data, orient='row').select(
        timestamp=(pl.col.date * 1000).cast(pl.Datetime('ms')),
        tvl_usd='tvl',
    )


def get_tvl_of_chain(chain: str) -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_tvl_of_chain', {'chain': chain})
    return pl.DataFrame(data).select(
        timestamp=(pl.col.date * 1000).cast(pl.Datetime('ms')),
        chain=pl.lit(chain),
        tvl_usd='tvl',
    )


def get_historical_tvl_per_chain_of_protocol(
    protocol: str, *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch(
            'historical_tvl_of_protocol', {'protocol': protocol}
        )
    rows = [
        [datum['date'], chain, protocol, datum['totalLiquidityUSD']]
        for chain in data['chainTvls']
        for datum in data['chainTvls'][chain]['tvl']
    ]
    schema = ['timestamp', 'chain', 'protocol', 'supply_usd']
    return pl.DataFrame(rows, schema=schema, orient='row').with_columns(
        (pl.col.timestamp * 1000).cast(pl.Datetime('ms'))
    )


def get_historical_tvl_per_token_of_protocol(
    protocol: str, *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch(
            'historical_tvl_of_protocol', {'protocol': protocol}
        )

    rows = [
        [datum['date'], protocol, symbol, value]
        for datum in data['tokens']
        for symbol, value in datum['tokens'].items()
    ]
    schema = ['timestamp', 'protocol', 'symbol', 'supply']
    tokens = (
        pl.DataFrame(rows, schema=schema, orient='row')
        .with_columns((pl.col.timestamp * 1000).cast(pl.Datetime('ms')))
        .sort('timestamp', 'symbol')
    )

    rows = [
        [datum['date'], protocol, symbol, value]
        for datum in data['tokensInUsd']
        for symbol, value in datum['tokens'].items()
    ]
    schema = ['timestamp', 'protocol', 'symbol', 'tvl_usd']
    tokensInUsd = (
        pl.DataFrame(rows, schema=schema, orient='row')
        .with_columns((pl.col.timestamp * 1000).cast(pl.Datetime('ms')))
        .sort('timestamp', 'protocol', 'symbol')
    )

    return tokens.join(
        tokensInUsd, on=['timestamp', 'protocol', 'symbol'], how='inner'
    )


def get_historical_tvl_of_protocol(
    protocol: str, *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch(
            'historical_tvl_of_protocol', {'protocol': protocol}
        )
    rows = [
        [int(datum['date']), protocol, float(datum['totalLiquidityUSD'])]
        for datum in data['tvl']
    ]
    return pl.DataFrame(
        rows, schema=['timestamp', 'protocol', 'tvl_usd'], orient='row'
    ).with_columns((pl.col.timestamp * 1000).cast(pl.Datetime('ms')))
