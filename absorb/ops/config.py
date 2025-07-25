from __future__ import annotations

import typing

import absorb
from . import paths

if typing.TYPE_CHECKING:
    import typing_extensions


def get_default_config() -> absorb.Config:
    import os
    import subprocess

    output = subprocess.check_output(['which', 'git'], text=True)
    use_git = os.path.isfile(output.strip())

    return {
        'version': absorb.__version__,
        'tracked_tables': [],
        'use_git': use_git,
        'default_bucket': {
            'provider': None,
            'bucket_name': None,
            'rclone_remote': None,
            'path_prefix': None,
        },
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


def write_config(
    config: absorb.Config, commit_message: str | None = None
) -> None:
    import json
    import os

    default_config = get_default_config()
    default_config.update(config)
    config = default_config

    if not validate_config(config):
        raise Exception('invalid config format')

    path = paths.get_config_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # load old config for change detection
    if os.path.isfile(path):
        with open(path, 'r') as f:
            old_config = json.load(f)
    else:
        old_config = None

    # write config
    with open(path, 'w') as f:
        json.dump(config, f)

    # version control
    if config['use_git']:
        if json.dumps(config, sort_keys=True) != json.dumps(
            old_config, sort_keys=True
        ):
            absorb.ops.git_add_and_commit_file(
                paths.get_config_path(),
                repo_root=paths.get_absorb_root(),
                message=commit_message,
            )


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


def start_tracking_tables(
    tables: typing.Sequence[absorb.TableReference],
) -> None:
    import json

    config = get_config()
    tracked_tables = {
        json.dumps(table, sort_keys=True) for table in config['tracked_tables']
    }
    for table in tables:
        instance = absorb.Table.instantiate(table)
        table_dict = instance.create_table_dict()
        as_str = json.dumps(table_dict, sort_keys=True)

        # check for name collisions
        for tracked_table in config['tracked_tables']:
            if table_dict['table_name'] == tracked_table[
                'table_name'
            ] and as_str != json.dumps(tracked_table, sort_keys=True):
                raise Exception(
                    'name collision, cannot add new tracked table: '
                    + str(table_dict['table_name'])
                )

        # add to tracked tables
        if as_str not in tracked_tables:
            config['tracked_tables'].append(table_dict)
            tracked_tables.add(as_str)

    names = ', '.join(
        absorb.Table.instantiate(table).full_name() for name in tables
    )
    message = 'Start tracking ' + str(len(tables)) + ' tables: ' + names
    write_config(config, message)


def stop_tracking_tables(
    tables: typing.Sequence[absorb.TableReference],
) -> None:
    import json

    drop_these = set()
    for table in tables:
        table_dict = absorb.Table.instantiate(table).create_table_dict()
        table_str = json.dumps(table_dict, sort_keys=True)
        drop_these.add(table_str)

    config = get_config()
    config['tracked_tables'] = [
        table
        for table in config['tracked_tables']
        if json.dumps(table, sort_keys=True) not in drop_these
    ]

    names = ', '.join(
        absorb.Table.instantiate(table).full_name() for table in tables
    )
    message = 'Stop tracking ' + str(len(tables)) + ' tables: ' + names
    write_config(config, message)


def enable_git_tracking() -> None:
    config = get_config()
    config['use_git'] = True
    write_config(config, 'Enable git tracking')


def disable_git_tracking() -> None:
    config = get_config()
    config['use_git'] = False
    write_config(config, 'Disable git tracking')


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


def set_default_rclone_remote(rclone_remote: str) -> None:
    config = get_config()
    config['default_bucket']['rclone_remote'] = rclone_remote
    write_config(config, 'Set default rclone remote to ' + rclone_remote)


def set_default_bucket(bucket: str) -> None:
    config = get_config()
    config['default_bucket']['bucket_name'] = bucket
    write_config(config, 'Set default bucket to ' + bucket)


def set_default_provider(provider: str) -> None:
    config = get_config()
    config['default_bucket']['provider'] = provider
    write_config(config, 'Set default provider to ' + provider)


def set_default_path_prefix(path_prefix: str) -> None:
    config = get_config()
    config['default_bucket']['path_prefix'] = path_prefix
    write_config(config, 'Set default path prefix to ' + path_prefix)


def clear_default_rclone_remote() -> None:
    config = get_config()
    config['default_bucket']['rclone_remote'] = None
    write_config(config, 'Cleared default rclone remote')


def clear_default_bucket() -> None:
    config = get_config()
    config['default_bucket']['bucket_name'] = None
    write_config(config, 'Cleared default bucket')


def clear_default_provider() -> None:
    config = get_config()
    config['default_bucket']['provider'] = None
    write_config(config, 'Cleared default provider')


def clear_default_path_prefix() -> None:
    config = get_config()
    config['default_bucket']['path_prefix'] = None
    write_config(config, 'Cleared default path prefix')
