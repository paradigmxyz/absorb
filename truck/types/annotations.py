from __future__ import annotations

import typing


RangeFormat = typing.Literal[
    'date',
    'date_range',
    'named_range',
    'block_range',
    'id_range',
    'count',
    None,
]


class Context(typing.TypedDict):
    parameters: dict[str, typing.Any]
    data_range: typing.Any
    overwrite: bool


class TrackedTable(typing.TypedDict):
    source_name: str
    table_name: str
    table_class: TableReference
    parameters: dict[str, typing.Any]


TableReference = typing.Any


class TruckConfig(typing.TypedDict):
    version: str
    tracked_tables: list[TrackedTable]
