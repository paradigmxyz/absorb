from __future__ import annotations

import typing

import absorb
from .. import cli_outputs
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def setup_command(args: Namespace) -> dict[str, Any]:
    if len(args.dataset) > 0:
        datasets = cli_parsing._parse_datasets(args)
    else:
        datasets = [
            absorb.Table.instantiate(table_dict)
            for table_dict in absorb.ops.get_tracked_tables()
        ]

    if args.regenerate_metadata:
        if len(datasets) == 1:
            word = 'dataset'
        else:
            word = 'datasets'
        print('generating metadata for', len(datasets), word)
        for dataset in datasets:
            instance = absorb.Table.instantiate(dataset)
            instance.setup_table_dir()

    if args.regenerate_config:
        old_config = absorb.ops.get_config()
        new_config = absorb.ops.get_default_config()
        for tracked_table in old_config['tracked_tables']:
            table = absorb.Table.instantiate(tracked_table)
            table_dict = table.create_table_dict()
            new_config['tracked_tables'].append(table_dict)
        absorb.ops.write_config(new_config)

    config = absorb.ops.get_config()
    if config['use_git']:
        setup_git(datasets=datasets)

    return {}


def setup_git(datasets: list[absorb.Table]) -> None:
    import os

    absorb_root = absorb.ops.get_absorb_root()

    # initialize repo
    if not absorb.ops.git_is_repo_root(absorb_root):
        absorb.ops.git_initialize_repo(absorb_root)

    # setup gitignore
    gitignore_path = os.path.join(absorb_root, '.gitignore')
    if not os.path.isfile(gitignore_path):
        default_gitignore = '*.parquet'
        with open(gitignore_path, 'w') as f:
            f.write(default_gitignore)
    if not absorb.ops.git_is_file_tracked(
        gitignore_path, repo_root=absorb_root
    ):
        absorb.ops.git_add_and_commit_file(
            gitignore_path, repo_root=absorb_root
        )

    # add config file
    config_path = absorb.ops.get_config_path()
    if not absorb.ops.git_is_file_tracked(config_path, repo_root=absorb_root):
        absorb.ops.git_add_and_commit_file(config_path, repo_root=absorb_root)

    # add metadata of existing tables
    n_added = 0
    for dataset in datasets:
        metadata_path = absorb.ops.get_table_metadata_path(dataset)
        if not absorb.ops.git_is_file_tracked(
            metadata_path, repo_root=absorb_root
        ):
            absorb.ops.git_add_file(metadata_path, repo_root=absorb_root)
            n_added += 1
    if n_added > 0:
        absorb.ops.git_commit(
            message='added ' + str(n_added) + ' table metadata files',
            repo_root=absorb_root,
        )
