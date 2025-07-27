from __future__ import annotations

import typing
import polars as pl
import absorb


class BaseQuery(absorb.Table):
    source = 'dune'
    url = 'https://dune.com/'
    name_template = [
        '{name}',
        'query_{query_id}',
    ]
    required_packages = ['dune_spice >= 0.2.6']
    required_credentials = ['DUNE_API_KEY']

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import spice

        query = self.parameters['query']
        spice_kwargs = self.parameters['spice_kwargs']
        spice_kwargs['limit'] = 0
        return dict(spice.query(query, **spice_kwargs).schema)


class FullQuery(BaseQuery):
    """collect the full output of a query"""

    description = 'Dune query'
    write_range = 'overwrite_all'
    parameter_types = {
        'name': str,
        'query_id': str,
        'spice_kwargs': dict[str, typing.Any],
    }
    required_packages = ['dune_spice >= 0.2.6']
    required_credentials = ['DUNE_API_KEY']

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import spice

        query = self.parameters['query']
        spice_kwargs = self.parameters['spice_kwargs']
        return spice.query(
            query, poll=True, include_execution=False, **spice_kwargs
        )


class AppendOnlyQuery(BaseQuery):
    """collect the output of a query, time-partitioned"""

    description = 'Dune query has an append-only structure'
    write_range = 'append_only'
    parameter_types = {
        'name': str,
        'query_id': str,
        'spice_kwargs': dict[str, typing.Any],
        'range_parameters': list[str],
    }
    required_packages = ['dune_spice >= 0.2.6']
    required_credentials = ['DUNE_API_KEY']

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import spice

        query = self.parameters['query']
        spice_kwargs = self.parameters['spice_kwargs']
        spice_kwargs.setdefault('parameters', {})
        self.parameters.update(chunk)  # type: ignore
        return spice.query(
            query, poll=True, include_execution=False, **spice_kwargs
        )


class CexLabels(absorb.Table):
    source = 'dune'
    description = 'CEX labels for EVM and Solana addresses'
    url = 'https://dune.com/'
    write_range = 'overwrite_all'
    index_type = 'id'
    index_column = 'address'
    required_packages = ['dune_spice >= 0.2.6']
    required_credentials = ['DUNE_API_KEY']

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'address': pl.String,
            'cex_name': pl.String,
            'distinct_name': pl.String,
            'added_by': pl.String,
            'added_date': pl.String,
            'ecosystem': pl.String,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import spice

        evm_cex_query = 'https://dune.com/queries/3237025'
        solana_cex_query = 'https://dune.com/queries/5124188'
        evm_cexes = spice.query(evm_cex_query).with_columns(
            ecosystem=pl.lit('EVM')
        )
        solana_cexes = (
            spice.query(solana_cex_query)
            .drop('blockchain')
            .with_columns(ecosystem=pl.lit('solana'))
        )
        return pl.concat([evm_cexes, solana_cexes])


def get_tables() -> list[type[absorb.Table]]:
    return [CexLabels]
