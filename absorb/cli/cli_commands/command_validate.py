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
    metadatas = {}

    # check that config is valid
    config = absorb.ops.get_config()
    default_config = absorb.ops.get_default_config()
    assert config.keys() == default_config.keys(), (
        'config keys do not match default config keys'
    )
    for key, value in default_config.items():
        if isinstance(value, dict):
            assert config[key].keys() == value.keys(), (  # type: ignore
                f'config {key} subkeys do not match subkeys of '
                + ', '.join(value.keys())
            )

    # check that datasets_dir exists
    datasets_dir = absorb.ops.get_datasets_dir()
    if not os.path.isdir(datasets_dir):
        print('no datasets collected')
        sys.exit(0)

    # check each collected dataset
    for source in os.listdir(datasets_dir):
        tables_dir = absorb.ops.get_source_tables_dir(source)
        for table in os.listdir(tables_dir):
            # check that each dataset has a metadata file
            table_dir = os.path.join(tables_dir, table)
            metadata_path = absorb.ops.get_table_metadata_path(
                table, source=source
            )

            # load metadata
            if os.path.isfile(metadata_path):
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                metadatas[table_dir] = metadata
            else:
                data_glob = glob.glob(os.path.join(table_dir, '*.parquet'))
                if len(data_glob) > 0:
                    errors.append(
                        f'{source}/{table} has parquet files but no metadata file'
                    )
                continue

            # validate that metadata is valid
            errors += [
                source + '.' + table + ' ' + error
                for error in absorb.ops.validate_table_dict(metadata)
            ]

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
            target_parquet_files = glob.glob(instance.get_data_glob())
            for filename in os.listdir(table_dir):
                if filename == os.path.basename(metadata_path):
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

    # validate tracked datasets
    config = absorb.ops.get_config()
    for metadata in config['tracked_tables']:
        # check that metadata is valid
        errors += [
            metadata.get('source_name', 'unknown_source')
            + '.'
            + metadata.get('table_name', 'unknown_table')
            + ' '
            + error
            for error in absorb.ops.validate_table_dict(metadata)
        ]

        # check that tracked metadata directory exists
        table_dir = absorb.ops.get_table_dir(metadata)
        if not os.path.isdir(table_dir):
            errors.append(
                metadata.get('source_name', 'unknown_source')
                + '.'
                + metadata.get('table_name', 'unknown_table')
                + ' directory does not exist: '
                + table_dir
            )
            continue

        # check that tracked metadata matches table folder metadata
        if json.dumps(metadata, sort_keys=True) != json.dumps(
            metadatas.get(table_dir, {}), sort_keys=True
        ):
            errors.append(
                metadata.get('source_name', 'unknown_source')
                + '.'
                + metadata.get('table_name', 'unknown_table')
                + ' tracked metadata does not match table folder metadata'
            )

    # check that when write_mode=overwrite_all there is at most one data file
    for table_dict in absorb.ops.get_collected_tables():
        instance = absorb.Table.instantiate(table_dict)
        if instance.write_range == 'overwrite_all':
            n_data_files = len(glob.glob(instance.get_data_glob()))
            if n_data_files > 1:
                errors.append(
                    'table ' + instance.full_name() + ' has multiple data files'
                )

    # print errors
    if len(errors) > 0:
        toolstr.print_text_box('Errors Found', style='red')
        for e, error in enumerate(errors):
            toolstr.print_bullet('[red]' + error + '[/red]', number=e + 1)
    else:
        toolstr.print('no errors found', style='green')

    return {}
