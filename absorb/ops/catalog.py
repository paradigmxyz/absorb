from __future__ import annotations

import os
import typing
import absorb

from . import names

if typing.TYPE_CHECKING:
    import types
    import polars as pl


def get_source_module(source: str) -> types.ModuleType:
    import importlib

    return importlib.import_module(
        'absorb.datasets.' + names._camel_to_snake(source)
    )


def get_table_class(source: str, table_name: str) -> type[absorb.Table]:
    return getattr(get_source_module(source), names._snake_to_camel(table_name))  # type: ignore


def get_sources(*, snake: bool = True) -> list[str]:
    import absorb.datasets

    sources = [
        filename.rsplit('.py', maxsplit=1)[0]
        for filename in os.listdir(absorb.datasets.__path__[0])
        if not filename.startswith('__')
    ]

    if not snake:
        sources = [names._snake_to_camel(source) for source in sources]

    return sources


def get_source_tables(source: str) -> list[type[absorb.Table]]:
    module = get_source_module(source)
    if hasattr(module, 'get_tables'):
        return module.get_tables()  # type: ignore
    else:
        return [
            value
            for key, value in vars(module).items()
            if isinstance(value, type) and issubclass(value, absorb.Table)
        ]


def get_table_classes() -> list[type[absorb.Table]]:
    return [
        table_class
        for source in get_sources()
        for table_class in get_source_tables(source)
    ]


def resolve_table(
    reference: absorb.TableReference,
    *,
    parameters: dict[str, typing.Any] | None = None,
) -> absorb.Table:
    if isinstance(reference, absorb.Table):
        return reference

    elif isinstance(reference, dict):
        return absorb.Table.instantiate(reference)

    elif isinstance(reference, str):
        source, table = reference.split('.')
        camel_table = names._snake_to_camel(table)
        snake_table = names._camel_to_snake(table)
        if parameters is None:
            parameters = {}
        tracked_table: absorb.TableDict = {
            'source_name': source,
            'table_name': snake_table,
            'table_class': 'absorb.datasets.' + source + '.' + camel_table,
            'parameters': parameters,
        }
        return absorb.Table.instantiate(tracked_table)

    else:
        raise Exception()


def get_available_tables(
    *, exclude_parameters: bool = False
) -> list[absorb.TableDict]:
    tables = []
    for source in get_sources():
        for table in get_source_tables(source):
            if exclude_parameters and len(table.parameter_types) > 0:
                continue
            snake_table = absorb.ops.names._camel_to_snake(table.__name__)
            tracked_table: absorb.TableDict = {
                'source_name': source,
                'table_name': snake_table,
                'table_class': 'absorb.datasets.'
                + source
                + '.'
                + table.__name__,
                'parameters': {},
            }
            tables.append(tracked_table)
    return tables


def get_tables_df(
    include_available_range: bool = False,
) -> pl.DataFrame:
    """WIP

    features wanted
    - is tracked
    - collected range
    - available range
    """
    import datetime
    import polars as pl

    rows: list[dict[str, typing.Any]] = []
    for table in absorb.ops.get_available_tables():
        row: dict[str, typing.Any] = dict(table)
        table_class = absorb.ops.get_table_class(
            source=table['source_name'],
            table_name=table['table_name'],
        )
        row['parameter_names'] = list(table_class.parameter_types.keys())

        if include_available_range:
            if len(table_class.parameter_types) > 0:
                pass
            else:
                instance = absorb.ops.resolve_table(table)
                if table_class.index_type == 'day':
                    available_range = instance.get_available_range()
                    if available_range is None:
                        raise Exception()
                    start, end = available_range
                    if not isinstance(
                        start, datetime.datetime
                    ) or not isinstance(end, datetime.datetime):
                        raise Exception()
                    if start is not None:
                        start = start.replace(tzinfo=datetime.timezone.utc)
                    if end is not None:
                        end = end.replace(tzinfo=datetime.timezone.utc)
                    row['available_start_time'] = start
                    row['available_end_time'] = end
    tables = pl.DataFrame(rows, orient='row').drop('parameters')

    #     tables.join(
    #         pl.DataFrame(absorb.ops.get_tracked_tables())
    #         .drop("parameters")
    #         .with_columns(tracked=True),
    #         on=["source_name", "table_name", "table_class"],
    #         how="left",
    #     ).with_columns(pl.col.tracked.fill_null(False))

    return tables
