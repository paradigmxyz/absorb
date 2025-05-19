from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


def scan(
    dataset: absorb.TableReference,
    *,
    parameters: dict[str, typing.Any] | None = None,
    scan_kwargs: dict[str, typing.Any] | None = None,
) -> pl.LazyFrame:
    import polars as pl

    table = absorb.ops.resolve_table(dataset, parameters=parameters)
    glob = table.get_glob()
    if scan_kwargs is None:
        scan_kwargs = {}
    try:
        return pl.scan_parquet(glob, **scan_kwargs)
    except Exception as e:
        if e.args[0].startswith('expected at least 1 source'):
            raise Exception('no data to load for ' + str(dataset))
        else:
            raise e


def load(dataset: absorb.TableReference, **kwargs: typing.Any) -> pl.DataFrame:
    """kwargs are passed to scan()"""
    import polars as pl

    try:
        return scan(dataset=dataset, **kwargs).collect()
    except pl.exceptions.ComputeError as e:
        if e.args[0].startswith('expected at least 1 source'):
            raise Exception('no data to load for ' + str(dataset))
        else:
            raise e


def write_file(*, df: pl.DataFrame, path: str) -> None:
    import os
    import shutil

    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp_path = path + '_tmp'
    if path.endswith('.parquet'):
        df.write_parquet(tmp_path)
    elif path.endswith('.csv'):
        df.write_csv(tmp_path)
    else:
        raise Exception('invalid file extension')
    shutil.move(tmp_path, path)
