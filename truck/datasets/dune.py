from __future__ import annotations

import typing
import polars as pl
import truck


class Query(truck.Table):
    """collect the output of a query"""

    write_range = 'overwrite'
    parameters = {
        'query': str,
        'spice_kwargs': dict[str, typing.Any],
    }

    @classmethod
    def get_schema(cls, context: truck.Context) -> dict[str, type[pl.DataType]]:
        import spice

        query = context['parameters']['query']
        spice_kwargs = context['parameters']['spice_kwargs']
        spice_kwargs['limit'] = 0
        return dict(spice.query(query, **spice_kwargs).schema)

    @classmethod
    def collect(cls, context: truck.Context) -> pl.DataFrame:
        import spice

        query = context['parameters']['query']
        spice_kwargs = context['parameters']['spice_kwargs']
        return spice.query(
            query, poll=True, include_execution=False, **spice_kwargs
        )


class AppendOnlyQuery(truck.Table):
    """collect the output of a query, time-partitioned"""

    write_range = 'append_only'
    parameters = {
        'query': str,
        'spice_kwargs': dict[str, typing.Any],
        'range_parameters': list[str],
    }

    @classmethod
    def get_schema(cls, context: truck.Context) -> dict[str, type[pl.DataType]]:
        import spice

        query = context['parameters']['query']
        spice_kwargs = context['parameters']['spice_kwargs']
        spice_kwargs['limit'] = 0
        return dict(spice.query(query, **spice_kwargs).schema)

    @classmethod
    def collect(cls, context: truck.Context) -> pl.DataFrame:
        import spice

        query = context['parameters']['query']
        spice_kwargs = context['parameters']['spice_kwargs']
        spice_kwargs.setdefault('parameters', {})
        spice_kwargs['parameters'].update(context['data_range'])
        return spice.query(
            query, poll=True, include_execution=False, **spice_kwargs
        )
