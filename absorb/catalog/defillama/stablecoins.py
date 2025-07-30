from __future__ import annotations

import typing

import absorb
from . import common


if typing.TYPE_CHECKING:
    import polars as pl


class Stablecoins(absorb.Table):
    source = 'defillama'
    description = 'Total circulating stablecoins in USD'
    url = 'https://defillama.com/'
    write_range = 'overwrite_all'
    row_precision = 'day'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'circulating_usd': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return get_historical_total_stablecoins()


class StablecoinsOfChains(absorb.Table):
    source = 'defillama'
    description = 'Circulating stablecoins per chain in USD'
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
            'circulating_usd': pl.Float64,
            'minted_usd': pl.Float64,
            'bridged_usd': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import polars as pl

        chains = self.parameters['chains']
        if chains is None:
            chains = _get_stablecoin_chains()

        dfs = []
        print('collecting', len(chains), 'chains')
        for c, chain in enumerate(chains, start=1):
            print('[' + str(c) + ' / ' + str(len(chains)) + ']', chain)
            try:
                df = get_historical_stablecoins_of_chain(chain)
                dfs.append(df)
            except Exception:
                print('could not collect', chain)
        return pl.concat(dfs)


class StablecoinsOfTokens(absorb.Table):
    source = 'defillama'
    description = 'Circulating stablecoins per token and per chain in USD'
    url = 'https://defillama.com/'
    write_range = 'overwrite_all'
    parameter_types = {'tokens': (list, type(None))}
    default_parameters = {'tokens': None}
    row_precision = 'day'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'token': pl.String,
            'chain': pl.String,
            'circulating': pl.Float64,
            'unreleased': pl.Float64,
            'minted': pl.Float64,
            'bridged_to': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import polars as pl

        tokens = self.parameters['tokens']
        if tokens is None:
            tokens = _get_stablecoin_tokens()

        dfs = []
        print('collecting', len(tokens), 'tokens')
        for t, token in enumerate(tokens, start=1):
            print('[' + str(t) + ' / ' + str(len(tokens)) + ']', token)
            df = get_historical_stablecoins_per_chain_of_token(token)
            dfs.append(df)
        return pl.concat(dfs)


class StablecoinPrices(absorb.Table):
    source = 'defillama'
    description = 'Prices of stablecoins in USD over time'
    url = 'https://defillama.com/'
    write_range = 'overwrite_all'
    row_precision = 'day'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'token': pl.String,
            'price': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return get_historical_stablecoin_prices()


#
# # historical stablecoin getters
#


def _get_stablecoin_chains() -> list[str]:
    data = common._fetch('current_stablecoins')
    return sorted(chain['name'] for chain in data['chains'])


def _get_stablecoin_tokens() -> list[str]:
    data = common._fetch('current_stablecoins')
    return sorted(asset['id'] for asset in data['peggedAssets'])


def get_historical_total_stablecoins() -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_total_stablecoins')
    rows = [
        [datum['date'], sum(datum['totalCirculatingUSD'].values())]
        for datum in data
    ]
    schema = {'timestamp': pl.Float64, 'circulating_usd': pl.Float64}
    return pl.DataFrame(rows, schema=schema, orient='row').with_columns(
        (pl.col.timestamp.cast(float) * 1000000).cast(pl.Datetime('us', 'UTC'))
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
    schema = {
        'timestamp': pl.Float64,
        'chain': pl.String,
        'circulating_usd': pl.Float64,
        'minted_usd': pl.Float64,
        'bridged_Usd': pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema, orient='row').with_columns(
        (pl.col.timestamp.cast(float) * 1000000).cast(pl.Datetime('us', 'UTC'))
    )


def get_historical_stablecoins_of_token(token: str) -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_stablecoins_of_token', {'token': token})
    rows = [
        [datum['date'], data['symbol'], datum['circulating'][data['pegType']]]
        for datum in data['tokens']
    ]
    schema = {
        'timestamp': pl.Float64,
        'token': pl.String,
        'circulating': pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema, orient='row').with_columns(
        (pl.col.timestamp.cast(float) * 1000000).cast(pl.Datetime('us', 'UTC'))
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
    schema = {
        'timestamp': pl.Float64,
        'token': pl.String,
        'chain': pl.String,
        'circulating': pl.Float64,
        'unreleased': pl.Float64,
        'minted': pl.Float64,
        'bridged_to': pl.Float64,
    }

    return (
        pl.DataFrame(rows, schema=schema, orient='row')
        .with_columns(
            (pl.col.timestamp.cast(float) * 1000000).cast(
                pl.Datetime('us', 'UTC')
            )
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
    schema = {'timestamp': pl.Float64, 'token': pl.String, 'price': pl.Float64}
    return (
        pl.DataFrame(rows, schema=schema, orient='row')
        .filter(pl.col.timestamp > 0)
        .with_columns(
            (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC'))
        )
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
