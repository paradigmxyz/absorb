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
    table = absorb.Table.instantiate(dataset, parameters=parameters)
    return table.scan(parameters=parameters, scan_kwargs=scan_kwargs)


def load(
    dataset: absorb.TableReference,
    *,
    parameters: dict[str, typing.Any] | None = None,
    scan_kwargs: dict[str, typing.Any] | None = None,
) -> pl.DataFrame:
    """kwargs are passed to scan()"""
    table = absorb.Table.instantiate(dataset, parameters=parameters)
    return table.load(parameters=parameters, scan_kwargs=scan_kwargs)


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
