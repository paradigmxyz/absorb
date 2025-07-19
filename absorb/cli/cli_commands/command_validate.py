from __future__ import annotations

import typing

import absorb
from .. import cli_outputs

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def validate_command(args: Namespace) -> dict[str, Any]:
    import glob
    import json
    import os
    import sys
    import toolstr

    # collect errors
    instances = []
    errors = []

    # check that datasets_dir exists
    datasets_dir = absorb.ops.get_datasets_dir()
    if not os.path.isdir(datasets_dir):
        print('no datasets collected')
        sys.exit(0)

    # check each collected dataset
    for source in os.listdir(datasets_dir):
        source_dir = os.path.join(datasets_dir, source)
        tables_dir = os.path.join(source_dir, 'tables')
        for table in os.listdir(tables_dir):
            # check that each dataset has a table_metadata.json
            table_dir = os.path.join(tables_dir, table)
            metadata_path = os.path.join(table_dir, 'table_metadata.json')
            if not os.path.isfile(metadata_path):
                errors.append(
                    f'{source}/{table} is missing table_metadata.json'
                )

            # load metadata
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)

            # instantiate table
            instance = absorb.Table.instantiate(metadata)
            instances.append(instance)

            # check that metadata matches table definition
            if instance.source != metadata['source_name']:
                errors.append(
                    source
                    + '.'
                    + table
                    + ' metadata source '
                    + metadata['source_name']
                    + ' does not match table source'
                    + instance.source
                )
            if instance.name() != metadata['table_name']:
                errors.append(
                    source
                    + '.'
                    + table
                    + ' metadata table name '
                    + metadata['table_name']
                    + ' does not match instance name '
                    + instance.name()
                )
            if instance.full_class_name() != metadata['table_class']:
                errors.append(
                    source
                    + '.'
                    + table
                    + ' metadata class name '
                    + str(metadata['table_class'])
                    + ' does not match instance class name '
                    + instance.full_class_name()
                )
            if instance.version != metadata['table_version']:
                errors.append(
                    source
                    + '.'
                    + table
                    + ' metadata table version '
                    + str(metadata['table_version'])
                    + ' does not match instance table version '
                    + str(instance.version)
                )

            # check that metadata matches filesystem location
            if source != metadata['source_name']:
                errors.append(
                    source
                    + '.'
                    + table
                    + ' metadata source '
                    + metadata['source_name']
                    + ' does not match filesystem location '
                    + table_dir
                )
            if table != metadata['table_name']:
                errors.append(
                    source
                    + '.'
                    + table
                    + ' metadata table name '
                    + metadata['table_name']
                    + ' does not match filesystem location '
                    + table_dir
                )

            # check there are no extra files beyond metadata and parquet files
            target_parquet_files = glob.glob(instance.get_glob())
            for filename in os.listdir(table_dir):
                if filename == 'table_metadata.json':
                    continue
                path = os.path.join(table_dir, filename)
                if path not in target_parquet_files:
                    errors.append(
                        source
                        + '.'
                        + table
                        + ' has unexpected file '
                        + filename
                        + ' in directory '
                        + table_dir
                    )

    # print errors
    if len(errors) > 0:
        toolstr.print_text_box('Errors Found', style='red')
        for e, error in enumerate(errors):
            toolstr.print_bullet('[red]' + error + '[/red]', number=e + 1)
    else:
        toolstr.print('no errors found', style='green')

    return {}
