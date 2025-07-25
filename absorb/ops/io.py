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
