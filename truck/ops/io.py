from __future__ import annotations

import typing

import truck

if typing.TYPE_CHECKING:
    import polars as pl


def scan(dataset: truck.TableReference) -> pl.LazyFrame:
    import polars as pl

    table = truck.ops.resolve_table(dataset)
    glob = table.get_glob()
    return pl.scan_parquet(glob)


def load(dataset: truck.TableReference) -> pl.DataFrame:
    return scan(dataset=dataset).collect()
