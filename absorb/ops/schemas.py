from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


def get_schema(
    table: absorb.TableReference,
) -> dict[str, type[pl.DataType] | pl.DataType]:
    return absorb.Table.instantiate(table).get_schema()
