from __future__ import annotations

import typing

import absorb


def get_available_range(
    dataset: absorb.TableReference,
    *,
    parameters: dict[str, typing.Any] | None = None,
) -> absorb.Coverage | None:
    table = absorb.ops.resolve_table(dataset, parameters=parameters)
    return table.get_available_range()


def get_collected_range(
    dataset: absorb.TableReference,
    *,
    parameters: dict[str, typing.Any] | None = None,
) -> absorb.Coverage | None:
    table = absorb.ops.resolve_table(dataset, parameters=parameters)
    return table.get_collected_range()


def get_collected_tables() -> absorb.TrackedTable:
    import os

    datasets_dir = absorb.ops.get_datasets_dir()
    tables = []
    for source in os.listdir(datasets_dir):
        source_dir = os.path.join(datasets_dir, source)
        tables_dir = os.path.join(source_dir, 'tables')
        for table in os.listdir(tables_dir):
            # TODO: get true parameters of colleceted datasets
            # TODO: read this information from a metadata.json
            camel_table = absorb.ops.names._snake_to_camel(table)
            table_data = {
                'source_name': source,
                'table_name': table,
                'table_class': 'absorb.datasets.' + source + '.' + camel_table,
                'parameters': {}
            }
            tables.append(table_data)
    return tables
