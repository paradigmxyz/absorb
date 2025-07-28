from __future__ import annotations

import typing

import absorb
from . import common


if typing.TYPE_CHECKING:
    import polars as pl


# class Fees(absorb.Table):
#     source = 'defillama'
#     write_range = 'overwrite_all'
#     chunk_size = 'date_range'
#     parameter_types = {
#         'chains': typing.Union[list[str], None],
#         'parameters': typing.Union[list[str], None],
#     }
#     default_parameters = {'chains': None, 'protocols': None}
#     name_template = {
#         'default': 'fees',
#         'custom': {
#             '{chains}': 'chains_fees_{chains}',
#             '{protocols}': 'protocol_fees_{protocols}',
#         },
#     }

#     def get_schema(self) -> dict[str, pl.DataType | pl.DataType | typ ]:
#         import polars as pl

#         return {
#             'timestamp': pl.Datetime('ms'),
#             'chain': pl.String,
#             'protocol': pl.String,
#             'revenue_usd': pl.Int64,
#         }

#     def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
#         import polars as pl

#         if self.parameters['protocols'] is not None:
#             protocols = self.parameters['protocols']
#             if protocols is None:
#                 protocols = _get_fee_protocols()
#             dfs = []
#             print('collecting fees of', len(protocols), 'protocols')
#             for p, protocol in enumerate(protocols, start=1):
#                 print(
#                     '[' + str(p) + ' / ' + str(len(protocols)) + ']', protocol
#                 )
#                 df = get_historical_fees_per_chain_of_protocol(protocol)
#                 dfs.append(df)
#             print('done')
#             return pl.concat(dfs)
#         else:
#             chains = self.parameters['chains']
#             if chains is None:
#                 chains = _get_fee_chains()
#             dfs = []
#             print('collecting fees of', len(chains), 'chains')
#             for c, chain in enumerate(chains, start=1):
#                 print('[' + str(c) + ' / ' + str(len(chains)) + ']', chain)
#                 df = get_historical_fees_per_protocol_of_chain(chain)
#                 dfs.append(df)
#             print('done')
#             return pl.concat(dfs)


class Fees(absorb.Table):
    source = 'defillama'
    description = 'Total fees collected by protocols across all chains'
    url = 'https://defillama.com/'
    write_range = 'overwrite_all'
    row_precision = 'day'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'revenue_usd': pl.Int64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return get_historical_fees()


class ChainFees(absorb.Table):
    source = 'defillama'
    description = 'Total fees collected by each protocol and each chain'
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
            'revenue_usd': pl.Int64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
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


class FeesOfProtocols(absorb.Table):
    source = 'defillama'
    description = 'Total fees collected by each protocol and each chain'
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
            'revenue_usd': pl.Int64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
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
        .with_columns(
            (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC'))
        )
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
        .with_columns(
            (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC'))
        )
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
        .with_columns(
            (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC'))
        )
        .sort('timestamp')
    )
