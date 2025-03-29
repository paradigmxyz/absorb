from __future__ import annotations

import typing
import polars as pl
import truck


class Query(truck.Datatype):
    """collect the output of a query"""
    write_range = 'overwrite'
    parameters = {
        'query': str,
        'spice_kwargs': dict[str, typing.Any],
    }

    @classmethod
    def get_schema(cls, context: truck.Context) -> dict[str, pl.DataFrame]:
        query = context.get_parameter('query')
        spice_kwargs = context.get_parameter('spice_kwargs')
        spice_kwargs['limit'] = 0
        return dict(spice.collect(query, **spice_kwargs).schema)

    @classmethod
    def collect(cls, context: truck.Context) -> pl.DataFrame:
        import spice

        query = context.get_parameter('query')
        spice_kwargs = context.get_parameter('spice_kwargs')
        return spice.collect(query, **spice_kwargs)


class AppendOnlyQuery(truck.Datatype):
    """collect the output of a query, time-partitioned"""

    write_range = 'append_only'
    parameters = {
        'query': str,
        'spice_kwargs': dict[str, typing.Any],
        'range_parameters': list[str],
    }

    @classmethod
    def get_schema(cls, context: truck.Context) -> dict[str, pl.DataFrame]:
        query = context.get_parameter('query')
        spice_kwargs = context.get_parameter('spice_kwargs')
        spice_kwargs['limit'] = 0
        return dict(spice.collect(query, **spice_kwargs).schema)

    @classmethod
    def collect(cls, context: truck.Context) -> pl.DataFrame:
        import spice

        query = context.get_parameter('query')
        spice_kwargs = context.get('spice_kwargs')
        range_parameters = context.get('range_parameters')

        # set range parameters
        spice_kwargs.setdefault('parameters', {})
        data_range = context.get_range()
        for parameter in range_parameters:
            spice_kwargs[parameter] = data_range[parameter]

        return spice.collect(query, **spice_kwargs)
