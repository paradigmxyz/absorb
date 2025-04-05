from __future__ import annotations

import truck


def get_truck_root() -> str:
    import os

    path = os.environ.get('TRUCK_ROOT')
    if path is None or path == '':
        path = '~/truck'
    path = os.path.expanduser(path)
    return path


def get_config_path() -> str:
    import os

    return os.path.join(get_truck_root(), 'truck_config.json')


def get_config() -> truck.TruckConfig:
    import json

    with open(get_config_path(), 'r') as f:
        config: truck.TruckConfig = json.load(f)
        return config


def get_tracked_tables() -> list[truck.TrackedTable]:
    return get_config()['tracked_tables']
