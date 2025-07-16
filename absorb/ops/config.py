from __future__ import annotations

import typing

import absorb
from . import paths

if typing.TYPE_CHECKING:
    import typing_extensions


def get_default_config() -> absorb.Config:
    return {
        'version': '0.1.0',
        'tracked_tables': [],
    }


def get_config() -> absorb.Config:
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


def write_config(config: absorb.Config) -> None:
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
) -> typing_extensions.TypeGuard[absorb.Config]:
    return (
        isinstance(config, dict)
        and {'tracked_tables'}.issubset(set(config.keys()))
        and isinstance(config['tracked_tables'], list)
    )


#
# # specific accessors
#


def get_tracked_tables() -> list[absorb.TableDict]:
    return get_config()['tracked_tables']


def start_tracking_tables(tables: list[absorb.TableDict]) -> None:
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


def stop_tracking_tables(tables: list[absorb.TableDict]) -> None:
    import json

    tables_str = [json.dumps(table, sort_keys=True) for table in tables]

    config = get_config()
    config['tracked_tables'] = [
        table
        for table in config['tracked_tables']
        if json.dumps(table, sort_keys=True) not in tables_str
    ]

    write_config(config)


#
# # environment
#


def is_package_installed(package: str) -> bool:
    """
    Check if a package is installed, optionally with version constraints.

    Args:
        package: Package name with optional version specifier
                 e.g., 'numpy', 'numpy>=1.20', 'numpy==1.21.0'

    Returns:
        bool: True if package is installed and meets version requirements
    """
    import importlib.metadata
    import re

    # Parse package string to extract name and version specifier
    match = re.match(r'^([a-zA-Z0-9_-]+)\s*(.*)$', package.strip())
    if not match:
        return False

    package_name = match.group(1)
    version_spec = match.group(2).strip()

    try:
        # Get installed version
        installed_version = importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return False

    # If no version specifier, just check if installed
    if not version_spec:
        return True

    # Parse version specifier
    spec_match = re.match(r'^(==|!=|<=|>=|<|>|~=)\s*(.+)$', version_spec)
    if not spec_match:
        return False

    operator = spec_match.group(1)
    required_version = spec_match.group(2).strip()

    # Compare versions
    return compare_versions(installed_version, operator, required_version)


def compare_versions(installed: str, operator: str, required: str) -> bool:
    """Compare version strings."""
    import re

    # Convert version strings to tuples of integers for comparison
    def version_tuple(v: str) -> tuple[int, ...]:
        return tuple(int(x) for x in re.findall(r'\d+', v))

    installed_tuple = version_tuple(installed)
    required_tuple = version_tuple(required)

    if operator == '==':
        return installed_tuple == required_tuple
    elif operator == '!=':
        return installed_tuple != required_tuple
    elif operator == '>=':
        return installed_tuple >= required_tuple
    elif operator == '<=':
        return installed_tuple <= required_tuple
    elif operator == '>':
        return installed_tuple > required_tuple
    elif operator == '<':
        return installed_tuple < required_tuple
    elif operator == '~=':
        # Compatible release: version should be >= X.Y and < X+1.0
        if len(required_tuple) < 2:
            return False
        return (
            installed_tuple[: len(required_tuple) - 1] == required_tuple[:-1]
            and installed_tuple >= required_tuple
        )

    return False
