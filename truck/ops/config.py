from __future__ import annotations

import typing

import truck
from . import paths

if typing.TYPE_CHECKING:
    import typing_extensions


def get_default_config() -> truck.TruckConfig:
    return {
        'version': truck.__version__,
        'tracked_tables': [],
    }


def get_config() -> truck.TruckConfig:
    import json

    try:
        with open(paths.get_config_path(), 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        return get_default_config()

    default_config = get_default_config()
    default_config.update(config)
    config = default_config

    if validate_config(config):
        return config
    else:
        raise Exception('invalid config format')


def write_config(config: truck.TruckConfig) -> None:
    import json
    import os

    default_config = get_default_config()
    default_config.update(config)
    config = default_config

    if not validate_config(config):
        raise Exception('invalid config format')

    path = paths.get_config_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(config, f)


def validate_config(
    config: typing.Any,
) -> typing_extensions.TypeGuard[truck.TruckConfig]:
    return (
        isinstance(config, dict)
        and {'tracked_tables'}.issubset(set(config.keys()))
        and isinstance(config['tracked_tables'], list)
    )


#
# # specific accessors
#


def get_tracked_tables() -> list[truck.TrackedTable]:
    return get_config()['tracked_tables']


def start_tracking_tables(tables: list[truck.TrackedTable]) -> None:
    import json

    config = get_config()
    tracked_tables = {
        json.dumps(table, sort_keys=True) for table in config['tracked_tables']
    }
    for table in tables:
        as_str = json.dumps(table, sort_keys=True)
        if as_str not in tracked_tables:
            config['tracked_tables'].append(table)
            tracked_tables.add(as_str)
    write_config(config)


def stop_tracking_tables(tables: list[truck.TrackedTable]) -> None:
    import json

    tables_str = [json.dumps(table, sort_keys=True) for table in tables]

    config = get_config()
    config['tracked_tables'] = [
        table
        for table in config['tracked_tables']
        if json.dumps(table, sort_keys=True) not in tables_str
    ]

    write_config(config)
