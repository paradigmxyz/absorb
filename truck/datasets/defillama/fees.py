from __future__ import annotations

import typing

import truck
from . import common


if typing.TYPE_CHECKING:
    import polars as pl


class Fees(truck.Table):
    source = 'defillama'
    write_range = 'overwrite_all'
    range_format = 'date_range'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        return {
            'timestamp': pl.Datetime('ms'),
            'revenue_usd': pl.Int64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        return get_historical_fees()


class ChainFees(truck.Table):
    source = 'defillama'
    write_range = 'overwrite_all'
    parameter_types = {'chains': typing.Union[list[str], None]}
    default_parameters = {'chains': None}
    range_format = 'date_range'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('ms'),
            'chain': pl.String,
            'protocol': pl.String,
            'revenue_usd': pl.Int64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        import polars as pl

        chains = self.parameters['chains']
        if chains is None:
            chains = _get_fee_chains()
        dfs = []
        print('collecting', len(chains), 'chains')
        for c, chain in enumerate(chains, start=1):
            print('[' + str(c) + ' / ' + str(len(chains)) + ']', chain)
            df = get_historical_fees_per_protocol_of_chain(chain)
            df = df.select(
                'timestamp',
                pl.lit(chain).alias('chain'),
                'protocol',
                'revenue_usd',
            )
            dfs.append(df)
        print('done')
        return pl.concat(dfs)


class FeesOfProtocols(truck.Table):
    source = 'defillama'
    write_range = 'overwrite_all'
    parameter_types = {'protocols': typing.Union[list[str], None]}
    range_format = 'date_range'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('ms'),
            'chain': pl.String,
            'protocol': pl.String,
            'revenue_usd': pl.Int64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        import polars as pl

        protocols = self.parameters['protocols']
        if protocols is None:
            protocols = _get_fee_protocols()
        dfs = []
        print('collecting', len(protocols), 'protocols')
        for p, protocol in enumerate(protocols, start=1):
            print('[' + str(p) + ' / ' + str(len(protocols)) + ']', protocol)
            df = get_historical_fees_per_chain_of_protocol(protocol)
            dfs.append(df)
        print('done')
        return pl.concat(dfs)


def _get_fee_chains() -> list[str]:
    data = common._fetch('historical_fees')
    chains: list[str] = data['allChains']
    return chains


def _get_fee_protocols() -> list[str]:
    data = common._fetch('historical_fees')
    return list(set(protocol['slug'] for protocol in data['protocols']))


def get_historical_fees() -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_fees')

    return (
        pl.DataFrame(
            data['totalDataChart'],
            schema=['timestamp', 'revenue_usd'],
            orient='row',
            strict=False,
        )
        .with_columns((pl.col.timestamp * 1000).cast(pl.Datetime('ms')))
        .sort('timestamp')
    )


def get_historical_fees_per_protocol_of_chain(chain: str) -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_fees_per_chain', {'chain': chain})

    return (
        pl.DataFrame(
            [
                [time, chain, protocol, value]
                for time, item in data['totalDataChartBreakdown']
                for protocol, value in item.items()
            ],
            schema=['timestamp', 'chain', 'protocol', 'revenue_usd'],
            orient='row',
        )
        .with_columns((pl.col.timestamp * 1000).cast(pl.Datetime('ms')))
        .sort('timestamp')
    )


def get_historical_fees_per_chain_of_protocol(protocol: str) -> pl.DataFrame:
    import polars as pl

    data = common._fetch('historical_fees_per_protocol', {'protocol': protocol})

    return (
        pl.DataFrame(
            [
                [time, chain, protocol, value]
                for time, item in data['totalDataChartBreakdown']
                for chain, subitem in item.items()
                for _, value in subitem.items()
            ],
            schema=['timestamp', 'chain', 'protocol', 'revenue_usd'],
            orient='row',
        )
        .with_columns((pl.col.timestamp * 1000).cast(pl.Datetime('ms')))
        .sort('timestamp')
    )
