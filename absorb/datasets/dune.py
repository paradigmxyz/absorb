from __future__ import annotations

import typing
import polars as pl
import absorb


class BaseQuery(absorb.Table):
    source = 'dune'
    name_template = {'custom': '{name}'}
    required_packages = ['spice >= 0.2.3']

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import spice

        query = self.parameters['query']
        spice_kwargs = self.parameters['spice_kwargs']
        spice_kwargs['limit'] = 0
        return dict(spice.query(query, **spice_kwargs).schema)


class FullQuery(BaseQuery):
    """collect the full output of a query"""

    write_range = 'overwrite_all'
    parameter_types = {
        'name': str,
        'query': str,
        'spice_kwargs': dict[str, typing.Any],
    }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        import spice

        query = self.parameters['query']
        spice_kwargs = self.parameters['spice_kwargs']
        return spice.query(
            query, poll=True, include_execution=False, **spice_kwargs
        )


class AppendOnlyQuery(BaseQuery):
    """collect the output of a query, time-partitioned"""

    write_range = 'append_only'
    range_format = 'named_range'
    parameter_types = {
        'name': str,
        'query': str,
        'spice_kwargs': dict[str, typing.Any],
        'range_parameters': list[str],
    }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame | None:
        import spice

        query = self.parameters['query']
        spice_kwargs = self.parameters['spice_kwargs']
        spice_kwargs.setdefault('parameters', {})
        self.parameters.update(data_range)
        return spice.query(
            query, poll=True, include_execution=False, **spice_kwargs
        )


class CexLabels(absorb.Table):
    source = 'dune'
    write_range = 'overwrite_all'
    required_packages = ['cryo_manager']

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'address': pl.String,
            'cex_name': pl.String,
            'distinct_name': pl.String,
            'added_by': pl.String,
            'added_date': pl.Datetime('ms'),
            'ecosystem': pl.String,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame | None:
        import spice

        evm_cex_query = 'https://dune.com/queries/3237025'
        solana_cex_query = 'https://dune.com/queries/5124188'
        evm_cexes = spice.query(evm_cex_query).with_columns(ecosystem='EVM')
        solana_cexes = (
            spice.query(solana_cex_query)
            .drop('blockchain')
            .with_columns(ecosystem=pl.lit('solana'))
        )
        return pl.concat([evm_cexes, solana_cexes])


def get_tables() -> list[type[absorb.Table]]:
    return [CexLabels]
