from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


def scan(
    table: absorb.TableReference,
    *,
    bucket: bool | absorb.Bucket = False,
    scan_kwargs: dict[str, typing.Any] | None = None,
) -> pl.LazyFrame:
    if bucket:
        if isinstance(bucket, bool):
            bucket = absorb.ops.get_default_bucket()
        return absorb.ops.scan_bucket(
            table=table, bucket=bucket, scan_kwargs=scan_kwargs
        )
    else:
        table = absorb.Table.instantiate(table)
        return table.scan(scan_kwargs=scan_kwargs)


def load(
    table: absorb.TableReference,
    *,
    bucket: bool | absorb.Bucket = False,
    scan_kwargs: dict[str, typing.Any] | None = None,
) -> pl.DataFrame:
    """kwargs are passed to scan()"""
    table = absorb.Table.instantiate(table)
    return table.load(scan_kwargs=scan_kwargs)


def write_file(*, df: pl.DataFrame, path: str) -> None:
    import os
    import shutil

    dirname = os.path.dirname(path)
    if dirname != '':
        os.makedirs(dirname, exist_ok=True)

    tmp_path = path + '_tmp'
    if path.endswith('.parquet'):
        df.write_parquet(tmp_path)
    elif path.endswith('.csv'):
        df.write_csv(tmp_path)
    else:
        raise Exception('invalid file extension')
    shutil.move(tmp_path, path)


def delete_table_dir(table: absorb.Table, confirm: bool = False) -> None:
    import os
    import shutil

    if not confirm:
        raise absorb.ConfirmError(
            'use confirm=True to delete table and its data files'
        )

    table_dir = table.get_table_dir()
    if os.path.isdir(table_dir):
        shutil.rmtree(table_dir)

    if absorb.ops.get_config()['use_git']:
        absorb.ops.git_remove_and_commit_file(
            table.get_table_metadata_path(),
            repo_root=absorb.ops.get_absorb_root(),
            message='Remove table metadata for ' + table.full_name(),
        )


def delete_table_data(table: absorb.Table, confirm: bool = False) -> None:
    import os
    import glob

    if not confirm:
        raise absorb.ConfirmError(
            'use confirm=True to delete table and its data files'
        )

    data_glob = table.get_data_glob()
    for path in glob.glob(data_glob):
        os.remove(path)


def get_dir_size(path: str) -> int:
    import platform
    import subprocess

    system = platform.system()

    if system == 'Linux':
        # Linux has -b flag for bytes
        result = subprocess.run(
            ['du', '-sb', path], capture_output=True, text=True, check=True
        )
        return int(result.stdout.strip().split('\t')[0])

    elif system == 'Darwin':  # macOS
        # macOS outputs in 512-byte blocks
        result = subprocess.run(
            ['du', '-s', path], capture_output=True, text=True, check=True
        )
        blocks = int(result.stdout.strip().split('\t')[0])
        return blocks * 512

    else:
        raise NotImplementedError(
            'Unsupported operating system for get_dir_size'
        )
