from __future__ import annotations

import typing

import truck

if typing.TYPE_CHECKING:
    import polars as pl


def scan(
    dataset: truck.TableReference,
    *,
    scan_kwargs: dict[str, typing.Any] | None = None,
) -> pl.LazyFrame:
    import polars as pl

    table = truck.ops.resolve_table(dataset)
    glob = table.get_glob()
    if scan_kwargs is None:
        scan_kwargs = {}
    return pl.scan_parquet(glob, **scan_kwargs)


def load(dataset: truck.TableReference, **kwargs: typing.Any) -> pl.DataFrame:
    """kwargs are passed to scan()"""
    return scan(dataset=dataset, **kwargs).collect()


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
