from __future__ import annotations

import typing

import truck
from . import common


if typing.TYPE_CHECKING:
    import polars as pl


class Fees(truck.Table):
    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        return {
            'timestamp': pl.Datetime('ms'),
            'revenue_usd': pl.Int64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        data = common.get_url_data(common.endpoints['fees'])
        return extract_total_revenue(data)


class FeesPerChain(truck.Table):
    parameter_types = {'protocols': list[str]}

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
            url = common.endpoints['fees_per_chain'].format(chain=chain)
            data = common.get_url_data(url)
            df = extract_revenue_per_protocol(data)
            df = df.select('timestamp', pl.lit(chain), 'protocol', 'revenue')
            dfs.append(df)
        return pl.concat(dfs)


class FeesPerProtocol(truck.Table):
    parameter_types = {'protocols': list[str]}

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
            url = common.endpoints['fees_per_protocol'].format(
                protocol=protocol
            )
            data = common.get_url_data(url)
            df = extract_revenue_per_protocol_per_chain(data)
            dfs.append(df)
        return pl.concat(dfs)


def extract_total_revenue(data: dict[str, typing.Any]) -> pl.DataFrame:
    import polars as pl

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


def extract_revenue_per_protocol(data: dict[str, typing.Any]) -> pl.DataFrame:
    import polars as pl

    return (
        pl.DataFrame(
            [
                [time, protocol, value]
                for time, item in data['totalDataChartBreakdown']
                for protocol, value in item.items()
            ],
            schema=['timestamp', 'protocol', 'revenue_usd'],
            orient='row',
        )
        .with_columns((pl.col.timestamp * 1000).cast(pl.Datetime('ms')))
        .sort('timestamp')
    )


def extract_revenue_per_protocol_per_chain(
    data: dict[str, typing.Any],
) -> pl.DataFrame:
    import polars as pl

    return (
        pl.DataFrame(
            [
                [time, chain, protocol, value]
                for time, item in data['totalDataChartBreakdown']
                for chain, subitem in item.items()
                for protocol, value in subitem.items()
            ],
            schema=['timestamp', 'chain', 'protocol', 'revenue_usd'],
            orient='row',
        )
        .with_columns((pl.col.timestamp * 1000).cast(pl.Datetime('ms')))
        .sort('timestamp')
    )
