from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl

import absorb
from . import common


# class DexVolumes(absorb.Table):
#     source = 'defillama'
#     write_range = 'overwrite_all'
#     parameter_types = {
#         'protocols': typing.Union[list[str], None],
#         'chains': typing.Union[list[str], None],
#         'top_n_protocols': typing.Union[int, None],
#         'top_n_chains': typing.Union[int, None],
#     }
#     default_parameters = {'protocols': None, 'chains': None}
#     name_template = {
#         'default': 'dex_volumes',
#         'custom': {
#             'protocols': 'dex_volumes_{protocols}',
#             'chains': 'dex_volumes_{chains}',
#             'top_n_protocols': 'dex_volumes_top_{top_n_protocols}_protocols',
#             'top_n_chains': 'dex_volumes_top_{top_n_chains}_chains',
#         },
#     }
#     chunk_size = 'date_range'

#     def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType] ]:
#         return {
#             'timestamp': pl.Datetime('ms'),
#             'chain': pl.String,
#             'protocol': pl.String,
#             'volume_usd': pl.Float64,
#         }

#     def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
#         import polars as pl

#         chains = self.parameters['chains']
#         protocols = self.parameters['protocols']

#         dfs = []
#         if protocols is not None:
#             print('collecting', len(protocols), 'protocols')
#             for p, protocol in enumerate(protocols, start=1):
#                 print(
#                     '[' + str(p) + ' / ' + str(len(protocols)) + ']', protocol
#                 )
#                 try:
#                     df = get_historical_dex_volume_per_chain_of_protocol(
#                         protocol
#                     )
#                     dfs.append(df)
#                 except Exception:
#                     print('could not collect', protocol)
#         else:
#             if chains is None:
#                 chains = _get_dex_chains()
#             print('collecting', len(chains), 'chains')
#             for c, chain in enumerate(chains, start=1):
#                 print('[' + str(c) + ' / ' + str(len(chains)) + ']', chain)
#                 try:
#                     df = get_historical_dex_volume_per_protocol_of_chain(chain)
#                     dfs.append(df)
#                 except Exception:
#                     print('could not collect', chain)
#         return pl.concat(dfs)


# class OptionsVolumes(absorb.Table):
#     source = 'defillama'
#     write_range = 'overwrite_all'
#     parameter_types = {
#         'protocols': typing.Union[list[str], None],
#         'chains': typing.Union[list[str], None],
#     }
#     default_parameters = {'protocols': None, 'chains': None}
#     chunk_size = 'date_range'

#     def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
#         return {
#             'timestamp': pl.Datetime('ms'),
#             'chain': pl.String,
#             'protocol': pl.String,
#             'volume_usd': pl.Float64,
#         }

#     def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
#         import polars as pl

#         chains = self.parameters['chains']
#         protocols = self.parameters['protocols']

#         dfs = []
#         if protocols is not None:
#             print('collecting', len(protocols), 'protocols')
#             for p, protocol in enumerate(protocols, start=1):
#                 print(
#                     '[' + str(p) + ' / ' + str(len(protocols)) + ']', protocol
#                 )
#                 try:
#                     df = get_historical_options_volume_per_chain_of_protocol(
#                         protocol
#                     )
#                     dfs.append(df)
#                 except Exception:
#                     print('could not collect', protocol)
#         else:
#             if chains is None:
#                 chains = _get_options_chains()
#             print('collecting', len(chains), 'chains')
#             for c, chain in enumerate(chains, start=1):
#                 print('[' + str(c) + ' / ' + str(len(chains)) + ']', chain)
#                 try:
#                     df = get_historical_options_volume_per_protocol_of_chain(
#                         chain
#                     )
#                     dfs.append(df)
#                 except Exception:
#                     print('could not collect', chain)
#         return pl.concat(dfs)


class ProtocolDexVolumes(absorb.Table):
    source = 'defillama'
    description = 'Volume of each dex protocol on each chain in USD over time'
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
            'volume_usd': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import polars as pl

        protocols = self.parameters['protocols']
        if protocols is None:
            protocols = _get_dex_protocols()
        dfs = []
        print('collecting', len(protocols), 'protocols')
        for p, protocol in enumerate(protocols, start=1):
            print('[' + str(p) + ' / ' + str(len(protocols)) + ']', protocol)
            try:
                df = get_historical_dex_volume_per_chain_of_protocol(protocol)
                dfs.append(df)
            except Exception:
                print('could not collect', protocol)
        return pl.concat(dfs)


class ChainDexVolumes(absorb.Table):
    source = 'defillama'
    description = 'Volume of each dex protocol on each chain in USD over time'
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
            'protocol': pl.String,
            'volume_usd': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import polars as pl

        chains = self.parameters['chains']
        if chains is None:
            chains = _get_dex_chains()
        dfs = []
        print('collecting', len(chains), 'chains')
        for c, chain in enumerate(chains, start=1):
            print('[' + str(c) + ' / ' + str(len(chains)) + ']', chain)
            try:
                df = get_historical_dex_volume_per_protocol_of_chain(chain)
                dfs.append(df)
            except Exception:
                print('could not collect', chain)
        return pl.concat(dfs)


class ProtocolOptionsVolumes(absorb.Table):
    source = 'defillama'
    description = (
        'Volume of each options protocol on each chain in USD over time'
    )
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
            'volume_usd': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import polars as pl

        protocols = self.parameters['protocols']
        if protocols is None:
            protocols = _get_options_protocols()
        dfs = []
        print('collecting', len(protocols), 'protocols')
        for p, protocol in enumerate(protocols, start=1):
            print('[' + str(p) + ' / ' + str(len(protocols)) + ']', protocol)
            df = get_historical_options_volume_per_chain_of_protocol(protocol)
            dfs.append(df)
        return pl.concat(dfs)


class ChainOptionsVolumes(absorb.Table):
    source = 'defillama'
    description = (
        'Volume of each options protocol on each chain in USD over time'
    )
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
            'protocol': pl.String,
            'volume_usd': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import polars as pl

        chains = self.parameters['chains']
        if chains is None:
            chains = _get_options_chains()
        dfs = []
        print('collecting', len(chains), 'chains')
        for c, chain in enumerate(chains, start=1):
            print('[' + str(c) + ' / ' + str(len(chains)) + ']', chain)
            df = get_historical_options_volume_per_protocol_of_chain(chain)
            dfs.append(df)
        return pl.concat(dfs)


#
# # dex volumes
#


def _get_dex_chains() -> list[str]:
    return (
        get_current_dex_volume_per_protocol()['chains']
        .list.explode()
        .unique()
        .sort()
        .to_list()
    )


def _get_dex_protocols() -> list[str]:
    return (
        get_current_dex_volume_per_protocol()['protocol']
        .unique()
        .sort()
        .to_list()
    )


def _get_options_chains() -> list[str]:
    return (
        get_current_options_volume_per_protocol()['chains']
        .list.explode()
        .unique()
        .sort()
        .to_list()
    )


def _get_options_protocols() -> list[str]:
    return (
        get_current_options_volume_per_protocol()['protocol']
        .unique()
        .sort()
        .to_list()
    )


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
        'protocol': pl.String,
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
        'protocol': pl.String,
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
    ).with_columns((pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC')))


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
        (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC'))
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
        (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC')),
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
        (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC'))
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
        (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC')),
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
        (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC'))
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
