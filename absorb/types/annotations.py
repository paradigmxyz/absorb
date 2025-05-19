from __future__ import annotations

import typing
from . import table_class

if typing.TYPE_CHECKING:
    from typing_extensions import NotRequired


RangeFormat = typing.Literal[
    'date',
    'hour',
    'date_range',
    'per_hour',
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
    table_class: str
    parameters: dict[str, typing.Any]


TableReference = typing.Union[str, TrackedTable, table_class.Table]


class Config(typing.TypedDict):
    version: str
    tracked_tables: list[TrackedTable]


class NameTemplate(typing.TypedDict):
    default: NotRequired[str]
    custom: NotRequired[str | dict[str | tuple[str], str]]
