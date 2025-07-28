from __future__ import annotations

import typing

import absorb
from . import common

if typing.TYPE_CHECKING:
    import polars as pl


class ChainTvls(absorb.Table):
    source = 'defillama'
    description = 'TVL of each chain in USD over time'
    url = 'https://defillama.com/'
    write_range = 'overwrite_all'
    parameter_types = {'chains': (list, type(None))}
    default_parameters = {'chains': None}
    row_precision = 'day'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'chain': pl.String,
            'tvl_usd': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import polars as pl

        chains = self.parameters['chains']
        if chains is None:
            chains = _get_tvl_chains()
        print('collecting', len(chains), 'chains')
        dfs = []
        for c, chain in enumerate(chains, start=1):
            print('[' + str(c) + ' / ' + str(len(chains)) + ']', chain)
            try:
                df = get_historical_tvl_of_chain(chain)
            except Exception:
                print('could not collect ' + chain)
                continue
            dfs.append(df)
        return pl.concat(dfs)


class ProtocolTvls(absorb.Table):
    source = 'defillama'
    description = 'TVL of each protocol on each chain in USD over time'
    url = 'https://defillama.com/'
    write_range = 'overwrite_all'
    parameter_types = {'protocols': (list, type(None))}
    default_parameters = {'protocols': None}
    row_precision = 'day'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'chain': pl.String,
            'protocol': pl.String,
            'tvl_usd': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import polars as pl

        protocols = self.parameters['protocols']
        if protocols is None:
            protocols = _get_tvl_protocols()
        dfs = []
        print('collecting', len(protocols), 'protocols')
        for p, protocol in enumerate(protocols, start=1):
            print('[' + str(p) + ' / ' + str(len(protocols)) + ']', protocol)
            try:
                df = get_historical_tvl_per_chain_of_protocol(protocol)
                dfs.append(df)
            except Exception:
                print('could not collect', protocol)
                continue
        return pl.concat(dfs)


class ProtocolTvlsPerToken(absorb.Table):
    source = 'defillama'
    description = 'TVL of each token in each protocol in USD over time'
    url = 'https://defillama.com/'
    write_range = 'overwrite_all'
    parameter_types = {'protocols': (list, type(None))}
    default_parameters = {'protocols': None}
    row_precision = 'day'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'protocol': pl.String,
            'symbol': pl.String,
            'supply': pl.Float64,
            'tvl_usd': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import polars as pl

        protocols = self.parameters['protocols']
        if protocols is None:
            protocols = _get_tvl_protocols()
        dfs = []
        print('collecting', len(protocols), 'protocols')
        for p, protocol in enumerate(protocols, start=1):
            print('[' + str(p) + ' / ' + str(len(protocols)) + ']', protocol)
            df = get_historical_tvl_per_token_of_protocol(protocol)
            dfs.append(df)
        return pl.concat(dfs)


def _get_tvl_chains() -> list[str]:
    return (
        get_current_project_tvls()['chains']
        .list.explode()
        .unique()
        .sort()
        .drop_nulls()
        .to_list()
    )


def _get_tvl_protocols() -> list[str]:
    return (
        get_current_project_tvls()['protocol']
        .unique()
        .sort()
        .drop_nulls()
        .to_list()
    )


def get_current_project_tvls() -> pl.DataFrame:
    import polars as pl

    data = common._fetch('current_tvls')

    return pl.DataFrame(
        data, orient='row', infer_schema_length=len(data), strict=False
    ).select(
        pl.col('name').alias('protocol'),
        'slug',
        pl.col.parentProtocol.str.strip_prefix('parent#').alias('parent'),
        'category',
        (pl.col.listedAt * 1000000)
        .cast(pl.Datetime('us', 'UTC'))
        .alias('list_date'),
        'symbol',
        'chain',
        'chains',
        'url',
        'github',
        pl.col.tvl.cast(pl.Float64).alias('tvl_usd'),
    )


def get_historical_tvl() -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_tvl')
    return pl.DataFrame(data, orient='row').select(
        timestamp=(pl.col.date * 1000000).cast(pl.Datetime('us', 'UTC')),
        tvl_usd=pl.col.tvl.cast(pl.Float64),
    )


def get_historical_tvl_of_chain(chain: str) -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_tvl_of_chain', {'chain': chain})
    return pl.DataFrame(data).select(
        timestamp=(pl.col.date * 1000000).cast(pl.Datetime('us', 'UTC')),
        chain=pl.lit(chain),
        tvl_usd=pl.col.tvl.cast(pl.Float64),
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
        [datum['date'], chain, protocol, float(datum['totalLiquidityUSD'])]
        for chain in data['chainTvls']
        for datum in data['chainTvls'][chain]['tvl']
    ]
    schema = {
        'timestamp': pl.Float64,
        'chain': pl.String,
        'protocol': pl.String,
        'tvl_usd': pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema, orient='row').with_columns(
        (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC'))
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
        [datum['date'], protocol, symbol, float(value)]
        for datum in data['tokens']
        for symbol, value in datum['tokens'].items()
    ]
    schema = ['timestamp', 'protocol', 'symbol', 'supply']
    tokens = (
        pl.DataFrame(rows, schema=schema, orient='row')
        .with_columns(
            (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC'))
        )
        .sort('timestamp', 'symbol')
    )

    rows = [
        [datum['date'], protocol, symbol, float(value)]
        for datum in data['tokensInUsd']
        for symbol, value in datum['tokens'].items()
    ]
    schema = ['timestamp', 'protocol', 'symbol', 'tvl_usd']
    tokensInUsd = (
        pl.DataFrame(rows, schema=schema, orient='row')
        .with_columns(
            (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC'))
        )
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
