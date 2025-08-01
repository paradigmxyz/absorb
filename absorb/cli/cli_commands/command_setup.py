from __future__ import annotations

import typing

import absorb
from .. import cli_outputs
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def setup_command(args: Namespace) -> dict[str, Any]:
    import toolstr
    import os

    # get list of input tables
    if len(args.dataset) > 0:
        datasets = cli_parsing._parse_datasets(args)
    else:
        datasets = [
            absorb.Table.instantiate(table_dict)
            for table_dict in absorb.ops.get_tracked_tables()
        ]

    # regenerate table metadata
    if args.regenerate_metadata:
        if len(datasets) == 1:
            word = 'dataset'
        else:
            word = 'datasets'
        print('generating metadata for', len(datasets), word)
        for dataset in datasets:
            instance = absorb.Table.instantiate(dataset)
            instance.setup_table_dir()

    # regenerate config
    if args.regenerate_config:
        old_config = absorb.ops.get_config()
        new_config = absorb.ops.get_default_config()
        for tracked_table in old_config['tracked_tables']:
            table = absorb.Table.instantiate(tracked_table)
            table_dict = table.create_table_dict()
            new_config['tracked_tables'].append(table_dict)
        absorb.ops.write_config(new_config)

    # change git settings
    if args.disable_git:
        absorb.ops.disable_git_tracking()
        config = absorb.ops.get_config()
    if args.enable_git:
        absorb.ops.enable_git_tracking()
        config = absorb.ops.get_config()

    # setup git tracking
    config = absorb.ops.get_config()
    if config['use_git']:
        absorb.ops.setup_git(track_tables=datasets)

    if args.set_default_bucket:
        import json

        absorb.ops.set_default_bucket(json.loads(args.set_default_bucket))
    if args.set_default_rclone_remote:
        absorb.ops.set_default_rclone_remote(args.set_default_rclone_remote)
    if args.set_default_bucket_name:
        absorb.ops.set_default_bucket_name(args.set_default_bucket_name)
    if args.set_default_provider:
        absorb.ops.set_default_provider(args.set_default_provider)
    if args.set_default_path_prefix:
        absorb.ops.set_default_path_prefix(args.set_default_path_prefix)
    if args.clear_default_bucket:
        absorb.ops.clear_default_bucket()
    if args.clear_default_rclone_remote:
        absorb.ops.clear_default_rclone_remote()
    if args.clear_default_bucket_name:
        absorb.ops.clear_default_bucket_name()
    if args.clear_default_provider:
        absorb.ops.clear_default_provider()
    if args.clear_default_path_prefix:
        absorb.ops.clear_default_path_prefix()

    # print config
    toolstr.print_text_box(
        'Current config', style='green', text_style='bold white'
    )
    config = absorb.ops.get_config()
    for key, value in config.items():
        if key == 'tracked_tables':
            continue
        if isinstance(value, dict):
            absorb.ops.print_bullet(key=key, value='')
            for subkey, subvalue in value.items():
                absorb.ops.print_bullet(key=subkey, value=subvalue, indent=4)
        else:
            absorb.ops.print_bullet(key=key, value=str(value))
    print()
    names = [
        table['source_name'] + '.' + table['table_name']
        for table in config['tracked_tables']
    ]
    toolstr.print_header(
        'Tracked datasets (' + str(len(names)) + ')',
        style='green',
        text_style='bold white',
    )
    for n, name in enumerate(sorted(names)):
        if not args.verbose:
            if n == 5:
                print('...')
            if n > 4 and n != len(names) - 1:
                continue
        absorb.ops.print_bullet(key=None, value=name, number=n + 1)

    print()
    if 'ABSORB_ROOT' in os.environ:
        print('config stored at ABSORB_ROOT:', absorb.ops.get_absorb_root())
    else:
        print('using default config, set ABSORB_ROOT to a config')

    return {}
