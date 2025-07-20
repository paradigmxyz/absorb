from __future__ import annotations

import typing

import absorb


def get_available_range(
    dataset: absorb.TableReference,
    *,
    parameters: dict[str, typing.Any] | None = None,
) -> absorb.Coverage | None:
    table = absorb.Table.instantiate(dataset, parameters=parameters)
    return table.get_available_range()


def get_collected_range(
    dataset: absorb.TableReference,
    *,
    parameters: dict[str, typing.Any] | None = None,
) -> absorb.Coverage | None:
    table = absorb.Table.instantiate(dataset, parameters=parameters)
    return table.get_collected_range()


def get_collected_tables() -> list[absorb.TableDict]:
    import json
    import os

    datasets_dir = absorb.ops.get_datasets_dir()
    if not os.path.isdir(datasets_dir):
        return []

    tables = []
    for source in os.listdir(datasets_dir):
        source_dir = os.path.join(datasets_dir, source)
        tables_dir = os.path.join(source_dir, 'tables')
        for table in os.listdir(tables_dir):
            metadata_path = os.path.join(
                tables_dir, table, 'table_metadata.json'
            )
            with open(metadata_path) as f:
                table_data = json.load(f)
            tables.append(table_data)
    return tables


def get_untracked_collected_tables(
    *, tracked_datasets: list[absorb.TableDict] | None = None
) -> list[absorb.TableDict]:
    import json

    if tracked_datasets is None:
        tracked_datasets = absorb.ops.get_tracked_tables()
    hashed_tracked_datasets = {
        json.dumps(dataset, sort_keys=True) for dataset in tracked_datasets
    }
    return [
        dataset
        for dataset in get_collected_tables()
        if json.dumps(dataset, sort_keys=True) not in hashed_tracked_datasets
    ]
