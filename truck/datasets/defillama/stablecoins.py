from __future__ import annotations

import typing

import truck
from . import common


if typing.TYPE_CHECKING:
    import polars as pl


class TotalStablecoins(truck.Table):
    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        return get_historical_total_stablecoins()


class StablecoinsPerChain(truck.Table):
    parameter_types = {'chains': list}

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        import polars as pl

        dfs = []
        for chain in self.parameters['chains']:
            df = get_historical_stablecoins_of_chain(chain)
            dfs.append(df)
        return pl.concat(dfs)


class StablecoinsPerToken(truck.Table):
    parameter_types = {'tokens': list}

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        import polars as pl

        dfs = []
        for token in self.parameters['tokens']:
            df = get_historical_stablecoins_of_token(token)
            dfs.append(df)
        return pl.concat(dfs)


class StablecoinsPerTokenPerChain(truck.Table):
    parameter_types = {'tokens': list}

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        import polars as pl

        dfs = []
        for token in self.parameters['tokens']:
            df = get_historical_stablecoins_per_chain_of_token(token)
            dfs.append(df)
        return pl.concat(dfs)


class StablecoinPrices(truck.Table):
    write_range = 'overwrite'

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        return get_historical_stablecoin_prices()


#
# # historical stablecoin getters
#


def get_historical_total_stablecoins() -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_total_stablecoins')
    rows = [
        [datum['date'], sum(datum['totalCirculatingUSD'].values())]
        for datum in data
    ]
    schema = ['timestamp', 'circulating_usd']
    return pl.DataFrame(rows, schema=schema, orient='row').with_columns(
        (pl.col.timestamp.cast(float) * 1000).cast(pl.Datetime('ms'))
    )


def get_historical_stablecoins_of_chain(chain: str) -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_stablecoins_of_chain', {'chain': chain})

    # timestamp, circulating_usd, minted_usd, bridged_usd, chain
    rows = [
        [
            datum['date'],
            chain,
            sum(datum['totalCirculatingUSD'].values()),
            sum(datum.get('totalMintedUSD', {'': 0}).values()),
            sum(datum.get('totalBridgedToUSD', {'': 0}).values()),
        ]
        for datum in data
    ]
    schema = [
        'timestamp',
        'chain',
        'circulating_usd',
        'minted_usd',
        'bridged_usd',
    ]

    return pl.DataFrame(rows, schema=schema, orient='row').with_columns(
        (pl.col.timestamp.cast(float) * 1000).cast(pl.Datetime('ms'))
    )


def get_historical_stablecoins_of_token(token: str) -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_stablecoins_of_token', {'token': token})
    rows = [
        [datum['date'], data['symbol'], datum['circulating'][data['pegType']]]
        for datum in data['tokens']
    ]
    schema = ['timestamp', 'token', 'circulating']
    return pl.DataFrame(rows, schema=schema, orient='row').with_columns(
        (pl.col.timestamp.cast(float) * 1000).cast(pl.Datetime('ms'))
    )


def get_historical_stablecoins_per_chain_of_token(token: str) -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_stablecoins_of_token', {'token': token})
    balances = data['chainBalances']
    peg_type = data['pegType']
    empty = {peg_type: 0}
    rows = [
        [
            datum['date'],
            data['symbol'],
            chain,
            datum.get('circulating', empty).get(peg_type, 0),
            datum.get('unreleased', empty).get(peg_type, 0),
            datum.get('minted', empty).get(peg_type, 0),
            datum.get('bridgedTo', empty).get(peg_type, 0),
        ]
        for chain in balances.keys()
        for datum in balances[chain]['tokens']
    ]
    schema = [
        'timestamp',
        'token',
        'chain',
        'circulating',
        'unreleased',
        'minted',
        'bridged_to',
    ]

    return (
        pl.DataFrame(rows, schema=schema, orient='row')
        .with_columns(
            (pl.col.timestamp.cast(float) * 1000).cast(pl.Datetime('ms'))
        )
        .sort('timestamp')
    )


def get_historical_stablecoin_prices() -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_stablecoin_prices')
    rows = [
        [datum['date'], str(token), float(price)]
        for datum in data
        for token, price in datum['prices'].items()
    ]
    return (
        pl.DataFrame(rows, schema=['timestamp', 'token', 'price'], orient='row')
        .filter(pl.col.timestamp > 0)
        .with_columns((pl.col.timestamp * 1000).cast(pl.Datetime('ms')))
    )


#
# # current stablecoin getters
#


def get_current_stablecoin_summary(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch('current_stablecoins')
    rows = []
    for asset in data['peggedAssets']:
        row = dict(asset)
        row['circulating'] = row['circulating'][row['pegType']]
        rows.append(row)
    return pl.DataFrame(rows).drop(
        'circulatingPrevDay',
        'circulatingPrevWeek',
        'circulatingPrevMonth',
        'chainCirculating',
        'chains',
    )


def get_current_stablecoins_per_chain_per_token(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch('current_stablecoins')
    rows = []
    for asset in data['peggedAssets']:
        for chain in asset['chainCirculating'].keys():
            row = dict(asset)
            row['chain'] = chain
            row['circulating'] = asset['chainCirculating'][chain]['current'][
                row['pegType']
            ]
            rows.append(row)
    return pl.DataFrame(rows).drop(
        'circulatingPrevDay',
        'circulatingPrevWeek',
        'circulatingPrevMonth',
        'chainCirculating',
        'chains',
    )


def get_current_stablecoins_per_chain(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch('current_stablecoins')
    rows = []
    for chain in data['chains']:
        row = dict(chain)
        rows.append(row)
    return pl.DataFrame(rows).select('name', tvl_usd='tvl').sort('name')


def get_current_stablecoins_per_chain_per_peg_type(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch('current_stablecoins')
    rows = []
    for chain in data['chains']:
        for peg, amount in chain['totalCirculatingUSD'].items():
            row = dict(chain)
            row['peg_type'] = peg
            row['circulating'] = amount
            rows.append(row)
    return (
        pl.DataFrame(rows)
        .select(
            'name',
            peg_type=pl.col.peg_type.str.strip_prefix('pegged'),
            circulating_native='circulating',
            tvl_usd='tvl',
        )
        .sort('name')
    )
