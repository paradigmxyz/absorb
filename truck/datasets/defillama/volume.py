from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl

import truck
from . import common


class DexVolumeOfProtocols(truck.Table):
    parameter_types = {'protocols': list[str]}
    range_format = 'date_range'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        return {
            'timestamp': pl.Datetime('ms'),
            'chain': pl.String,
            'protocol': pl.String,
            'volume_usd': pl.Float64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        dfs = [
            get_historical_dex_volume_per_chain_of_protocol(protocol)
            for protocol in self.parameters['protocols']
        ]
        return pl.concat(dfs)


class DexVolumeOfChains(truck.Table):
    parameter_types = {'chains': list[str]}
    range_format = 'date_range'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        return {
            'timestamp': pl.Datetime('ms'),
            'chain': pl.String,
            'protocol': pl.String,
            'volume_usd': pl.Float64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        dfs = [
            get_historical_dex_volume_per_protocol_of_chain(chain)
            for chain in self.parameters['chains']
        ]
        return pl.concat(dfs)


class OptionsVolumeOfProtocols(truck.Table):
    parameter_types = {'protocols': list[str]}
    range_format = 'date_range'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        return {
            'timestamp': pl.Datetime('ms'),
            'chain': pl.String,
            'protocol': pl.String,
            'volume_usd': pl.Float64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        dfs = [
            get_historical_options_volume_per_chain_of_protocol(protocol)
            for protocol in self.parameters['protocols']
        ]
        return pl.concat(dfs)


class OptionsVolumeOfChains(truck.Table):
    parameter_types = {'chains': list[str]}
    range_format = 'date_range'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        return {
            'timestamp': pl.Datetime('ms'),
            'chain': pl.String,
            'protocol': pl.String,
            'volume_usd': pl.Float64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        dfs = [
            get_historical_options_volume_per_protocol_of_chain(chain)
            for chain in self.parameters['chains']
        ]
        return pl.concat(dfs)


#
# # dex volumes
#


def get_current_dex_volume_per_protocol(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch('historical_dex_volume')

    rows = [
        [
            protocol['name'],
            protocol['displayName'],
            protocol['slug'],
            protocol['category'],
            protocol['chains'],
            protocol.get('total24h'),
            protocol.get('total48hto24h'),
            protocol.get('total7d'),
            protocol.get('total14dto7d'),
            protocol.get('total30d'),
            protocol.get('total1y'),
            protocol.get('totalAllTime'),
        ]
        for protocol in data['protocols']
    ]
    schema = {
        'name': pl.String,
        'displayName': pl.String,
        'slug': pl.String,
        'category': pl.String,
        'chains': None,
        'volume_24h_usd': pl.Float64,
        'volume_48h_to_24h_usd': pl.Float64,
        'volume_7d_usd': pl.Float64,
        'volume_14d_to_7d_usd': pl.Float64,
        'volume_30d_usd': pl.Float64,
        'volume_1y_usd': pl.Float64,
        'volume_all_time_usd': pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema, orient='row')


def get_current_dex_volume_per_chain_per_protocol(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch('historical_dex_volume')

    rows = []
    for protocol in data['protocols']:
        if protocol.get('breakdown24h') is None:
            continue
        for chain in protocol['breakdown24h'].keys():
            row = [
                chain,
                protocol['name'],
                protocol['displayName'],
                protocol['slug'],
                protocol['category'],
                protocol['protocolType'],
                sum(protocol['breakdown24h'][chain].values()),
                sum(protocol['breakdown30d'][chain].values()),
            ]
            rows.append(row)

    schema = {
        'chain': pl.String,
        'name': pl.String,
        'displayName': pl.String,
        'slug': pl.String,
        'category': pl.String,
        'protocolType': pl.String,
        'volume_24h_usd': pl.Float64,
        'volume_30d_usd': pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema, orient='row')


def get_historical_dex_volume(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch('historical_dex_volume')
    return pl.DataFrame(
        data['totalDataChart'], schema=['timestamp', 'volume_usd'], orient='row'
    ).with_columns((pl.col.timestamp * 1000).cast(pl.Datetime('ms')))


def get_historical_dex_volume_per_protocol(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch('historical_dex_volume')
    rows = [
        [timestamp, protocol, value]
        for timestamp, datum in data['totalDataChartBreakdown']
        for protocol, value in datum.items()
    ]
    schema = ['timestamp', 'protocol', 'volume_usd']
    return pl.DataFrame(rows, orient='row', schema=schema).with_columns(
        (pl.col.timestamp * 1000).cast(pl.Datetime('ms'))
    )


def get_historical_dex_volume_of_protocol(
    protocol: str, *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch(
            'historical_dex_volume_of_protocol', {'protocol': protocol}
        )
    return pl.DataFrame(
        data['totalDataChart'],
        schema={'timestamp': pl.Int64, 'volume_usd': pl.Float64},
        orient='row',
    ).select(
        (pl.col.timestamp * 1000).cast(pl.Datetime('ms')),
        pl.lit(data['name']).alias('protocol'),
        'volume_usd',
    )


def get_historical_dex_volume_per_chain_of_protocol(
    protocol: str, *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch(
            'historical_dex_volume_of_protocol', {'protocol': protocol}
        )
    rows = [
        [timestamp, chain, data['name'], sum(value.values())]
        for timestamp, datum in data['totalDataChartBreakdown']
        for chain, value in datum.items()
    ]
    schema = ['timestamp', 'chain', 'protocol', 'volume_usd']
    return pl.DataFrame(rows, orient='row', schema=schema).with_columns(
        (pl.col.timestamp * 1000).cast(pl.Datetime('ms'))
    )


def get_historical_dex_volume_of_chain(
    chain: str,
    *,
    data: pl.DataFrame | None = None,
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch('historical_dex_volume_of_chain', {'chain': chain})

    return pl.DataFrame(
        data['totalDataChart'], schema=['timestamp', 'volume_usd'], orient='row'
    ).select(
        (pl.col.timestamp * 1000).cast(pl.Datetime('ms')),
        pl.lit(chain).alias('chain'),
        'volume_usd',
    )


def get_historical_dex_volume_per_protocol_of_chain(
    chain: str, *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    import polars as pl

    if data is None:
        data = common._fetch('historical_dex_volume_of_chain', {'chain': chain})
    rows = [
        [timestamp, chain, protocol, value]
        for timestamp, datum in data['totalDataChartBreakdown']
        for protocol, value in datum.items()
    ]
    schema = ['timestamp', 'chain', 'protocol', 'volume_usd']
    return pl.DataFrame(rows, orient='row', schema=schema).with_columns(
        (pl.col.timestamp * 1000).cast(pl.Datetime('ms'))
    )


#
# # options volumes
#


def get_current_options_volume_per_protocol(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    if data is None:
        data = common._fetch('historical_options_volume')
    return get_current_dex_volume_per_protocol(data=data)


def get_current_options_volume_per_chain_per_protocol(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    if data is None:
        data = common._fetch('historical_options_volume')
    return get_current_dex_volume_per_chain_per_protocol(data=data)


def get_historical_options_volume(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    if data is None:
        data = common._fetch('historical_options_volume')
    return get_historical_dex_volume(data=data)


def get_historical_options_volume_per_protocol(
    *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    if data is None:
        data = common._fetch('historical_options_volume')
    return get_historical_dex_volume_per_protocol(data=data)


def get_historical_options_volume_of_protocol(
    protocol: str, *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    if data is None:
        data = common._fetch(
            'historical_options_volume_of_protocol', {'protocol': protocol}
        )
    return get_historical_dex_volume_of_protocol(protocol=protocol, data=data)


def get_historical_options_volume_per_chain_of_protocol(
    protocol: str, *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    if data is None:
        data = common._fetch(
            'historical_options_volume_of_protocol', {'protocol': protocol}
        )
    return get_historical_dex_volume_per_chain_of_protocol(
        protocol=protocol, data=data
    )


def get_historical_options_volume_of_chain(
    chain: str,
    *,
    data: pl.DataFrame | None = None,
) -> pl.DataFrame:
    if data is None:
        data = common._fetch(
            'historical_options_volume_of_chain', {'chain': chain}
        )
    return get_historical_dex_volume_of_chain(chain=chain, data=data)


def get_historical_options_volume_per_protocol_of_chain(
    chain: str, *, data: pl.DataFrame | None = None
) -> pl.DataFrame:
    if data is None:
        data = common._fetch(
            'historical_options_volume_of_chain', {'chain': chain}
        )
    return get_historical_dex_volume_per_protocol_of_chain(
        chain=chain, data=data
    )
