from __future__ import annotations

import typing

import absorb
from .. import paths
from . import config_io

if typing.TYPE_CHECKING:
    import typing_extensions


#
# # table tracking
#


def get_tracked_tables() -> list[absorb.TableDict]:
    return config_io.get_config()['tracked_tables']


def add(
    table: absorb.TableReference
    | list[absorb.TableReference]
    | list[absorb.Table],
) -> None:
    """start tracking tables"""
    import json

    if isinstance(table, list):
        tables = table
    else:
        tables = [table]

    table_objs = [absorb.Table.instantiate(table) for table in tables]
    config = config_io.get_config()
    tracked_tables = {
        json.dumps(table, sort_keys=True): table
        for table in config['tracked_tables']
    }

    # check for validity
    for table in table_objs:
        as_dict = table.create_table_dict()
        name = as_dict['table_name']
        as_str = json.dumps(as_dict, sort_keys=True)

        # check for name collisions
        for tracked_str, tracked_dict in tracked_tables.items():
            if name == tracked_dict['table_name'] and as_str != tracked_str:
                raise Exception('name collision, cannot add: ' + name)

        # check that table is registered
        if type(table) not in absorb.ops.get_source_table_classes(table.source):
            raise Exception('table ' + name + ' is not registered to source')

        # add to tracked tables if not already tracked
        if as_str not in tracked_tables:
            config['tracked_tables'].append(as_dict)
            tracked_tables[as_str] = as_dict

    # setup directory for each table
    for table in table_objs:
        table.setup_table_dir()

    # write new config
    names = ', '.join(table_obj.full_name() for table_obj in table_objs)
    message = 'Start tracking ' + str(len(tables)) + ' tables: ' + names
    config_io.write_config(config, message)


def remove(
    table: absorb.TableReference
    | list[absorb.TableReference]
    | list[absorb.Table],
) -> None:
    """stop tracking tables"""
    import json

    if isinstance(table, list):
        tables = table
    else:
        tables = [table]

    # gather tables to drop
    drop_tables = [absorb.Table.instantiate(table) for table in tables]
    drop_names = {table.name() for table in drop_tables}

    # create new tracked tables
    config = config_io.get_config()
    config['tracked_tables'] = [
        table
        for table in config['tracked_tables']
        if table['table_name'] not in drop_names
    ]

    # write new config
    names = ', '.join(table.full_name() for table in drop_tables)
    message = 'Stop tracking ' + str(len(drop_names)) + ' tables: ' + names
    config_io.write_config(config, message)


#
# # git tracking
#


def enable_git_tracking() -> None:
    config = config_io.get_config()
    config['use_git'] = True
    config_io.write_config(config, 'Enable git tracking')


def disable_git_tracking() -> None:
    config = config_io.get_config()
    config['use_git'] = False
    config_io.write_config(config, 'Disable git tracking')


#
# # bucket settings
#


def set_default_bucket(bucket: absorb.Bucket) -> None:
    # check bucket validity
    default_bucket = absorb.ops.get_default_config()['default_bucket']
    for key in bucket.keys():
        if key not in default_bucket:
            raise Exception('bucket has invalid key: ' + str(key))
    for key in default_bucket.keys():
        if key not in bucket:
            raise Exception('bucket is missing key: ' + str(key))

    config = config_io.get_config()
    config['default_bucket'] = bucket
    config_io.write_config(
        config, 'Set default bucket to ' + str(bucket['bucket_name'])
    )


def set_default_rclone_remote(rclone_remote: str) -> None:
    config = config_io.get_config()
    config['default_bucket']['rclone_remote'] = rclone_remote
    config_io.write_config(
        config, 'Set default rclone remote to ' + rclone_remote
    )


def set_default_bucket_name(bucket: str) -> None:
    config = config_io.get_config()
    config['default_bucket']['bucket_name'] = bucket
    config_io.write_config(config, 'Set default bucket to ' + bucket)


def set_default_provider(provider: str) -> None:
    config = config_io.get_config()
    config['default_bucket']['provider'] = provider
    config_io.write_config(config, 'Set default provider to ' + provider)


def set_default_path_prefix(path_prefix: str) -> None:
    config = config_io.get_config()
    config['default_bucket']['path_prefix'] = path_prefix
    config_io.write_config(config, 'Set default path prefix to ' + path_prefix)


def clear_default_bucket() -> None:
    config = config_io.get_config()
    config['default_bucket'] = absorb.ops.get_default_config()['default_bucket']
    config_io.write_config(config, 'Cleared default bucket')


def clear_default_rclone_remote() -> None:
    config = config_io.get_config()
    config['default_bucket']['rclone_remote'] = None
    config_io.write_config(config, 'Cleared default rclone remote')


def clear_default_bucket_name() -> None:
    config = config_io.get_config()
    config['default_bucket']['bucket_name'] = None
    config_io.write_config(config, 'Cleared default bucket')


def clear_default_provider() -> None:
    config = config_io.get_config()
    config['default_bucket']['provider'] = None
    config_io.write_config(config, 'Cleared default provider')


def clear_default_path_prefix() -> None:
    config = config_io.get_config()
    config['default_bucket']['path_prefix'] = None
    config_io.write_config(config, 'Cleared default path prefix')
