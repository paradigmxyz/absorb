from __future__ import annotations

import typing

import truck

if typing.TYPE_CHECKING:
    import typing_extensions


def get_config_path() -> str:
    import os

    return os.path.join(truck.get_truck_root(), 'truck_config.json')


def get_default_config() -> truck.TruckConfig:
    return {
        'version': truck.__version__,
        'tracked_tables': [],
    }


def get_config() -> truck.TruckConfig:
    import json

    try:
        with open(get_config_path(), 'r') as f:
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

    default_config = get_default_config()
    default_config.update(config)
    config = default_config

    if validate_config(config):
        with open(get_config_path(), 'w') as f:
            json.dump(config, f)
    else:
        raise Exception('invalid config format')


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


def create_tracked_tables(tables: list[truck.TrackedTable]) -> None:
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
