from __future__ import annotations

import typing

import truck
from . import common


if typing.TYPE_CHECKING:
    import polars as pl


class Fees(truck.Table):
    range_format = 'date_range'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        return {
            'timestamp': pl.Datetime('ms'),
            'revenue_usd': pl.Int64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        return get_historical_fees()


class ChainFees(truck.Table):
    parameter_types = {'chains': list[str]}
    range_format = 'date_range'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        return {
            'timestamp': pl.Datetime('ms'),
            'chain': pl.String,
            'protocol': pl.String,
            'revenue_usd': pl.Int64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        dfs = []
        for chain in self.parameters['chains']:
            df = get_historical_fees_per_protocol_of_chain(chain)
            df = df.select('timestamp', pl.lit(chain), 'protocol', 'revenue')
            dfs.append(df)
        return pl.concat(dfs)


class FeesOfProtocols(truck.Table):
    parameter_types = {'protocols': list[str]}
    range_format = 'date_range'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        return {
            'timestamp': pl.Datetime('ms'),
            'chain': pl.String,
            'protocol': pl.String,
            'revenue_usd': pl.Int64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        dfs = []
        for protocol in self.parameters['protocols']:
            df = get_historical_fees_per_chain_of_protocol(protocol)
            dfs.append(df)
        return pl.concat(dfs)


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
