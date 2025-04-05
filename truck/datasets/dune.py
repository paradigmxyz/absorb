from __future__ import annotations

import typing
import polars as pl
import truck


class Query(truck.Table):
    """collect the output of a query"""

    write_range = 'overwrite'
    parameter_types = {
        'query': str,
        'spice_kwargs': dict[str, typing.Any],
    }

    def get_schema(self) -> dict[str, type[pl.DataType]]:
        import spice

        query = self.parameters['query']
        spice_kwargs = self.parameters['spice_kwargs']
        spice_kwargs['limit'] = 0
        return dict(spice.query(query, **spice_kwargs).schema)

    def collect(self, data_range: typing.Any) -> pl.DataFrame:
        import spice

        query = self.parameters['query']
        spice_kwargs = self.parameters['spice_kwargs']
        return spice.query(
            query, poll=True, include_execution=False, **spice_kwargs
        )


class AppendOnlyQuery(truck.Table):
    """collect the output of a query, time-partitioned"""

    write_range = 'append_only'
    parameter_types = {
        'query': str,
        'spice_kwargs': dict[str, typing.Any],
        'range_parameters': list[str],
    }

    def get_schema(self) -> dict[str, type[pl.DataType]]:
        import spice

        query = self.parameters['query']
        spice_kwargs = self.parameters['spice_kwargs']
        spice_kwargs['limit'] = 0
        return dict(spice.query(query, **spice_kwargs).schema)

    def collect(self, data_range: typing.Any) -> pl.DataFrame:
        import spice

        query = self.parameters['query']
        spice_kwargs = self.parameters['spice_kwargs']
        spice_kwargs.setdefault('parameters', {})
        self.parameters.update(data_range)
        return spice.query(
            query, poll=True, include_execution=False, **spice_kwargs
        )
