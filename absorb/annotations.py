from __future__ import annotations

from typing_extensions import NotRequired
import typing
from typing import Union
import datetime
import types

import polars as pl

from . import table


#
# # coverage
#

IndexValue = Union[datetime.datetime, int, str]
IndexRange = Union[
    tuple[datetime.datetime, datetime.datetime],
    tuple[int, int],
    tuple[str, str],
]
IndexSemiRange = Union[
    tuple[Union[datetime.datetime, None], Union[datetime.datetime, None]],
    tuple[Union[int, None], Union[int, None]],
    tuple[Union[str, None], Union[str, None]],
]
CustomRange = dict[str, typing.Any]
Coverage = typing.Union[IndexRange, list[IndexRange], CustomRange]

#
# # index types
#

CustomIndexType = dict[str, typing.Any]
IndexType = typing.Union[
    typing.Literal['temporal', 'numerical', 'id', 'no_index'],
    CustomIndexType,
]

#
# # chunks
#

PrimitiveChunk = typing.Union[
    datetime.datetime,
    tuple[datetime.datetime, datetime.datetime],
    int,
    list[int],
    tuple[int, int],
    str,
    list[str],
    tuple[str, str],
]
CustomChunk = dict[str, typing.Any]
Chunk = typing.Union[None, PrimitiveChunk, CustomChunk]

#
# # chunk sizes
#

TemporalChunkSize = typing.Literal[
    'hour',
    'day',
    'week',
    'month',
    'quarter',
    'year',
]
NumericalChunkSize = int
CustomChunkSize = dict[str, typing.Any]
ChunkSize = typing.Union[
    TemporalChunkSize,
    NumericalChunkSize,
    CustomChunkSize,
]

#
# # chunk collection outputs
#


class ChunkPaths(typing.TypedDict):
    type: typing.Literal['files']
    paths: list[str]


ChunkResult = typing.Union[pl.DataFrame, ChunkPaths]

#
# # table representation
#

JSONValue = typing.Union[
    str,
    int,
    float,
    bool,
    None,
    dict[str, 'JSONValue'],
    list['JSONValue'],
]


class TableDict(typing.TypedDict):
    source_name: str
    table_name: str
    table_class: str
    parameters: dict[str, JSONValue]
    table_version: str


TableReference = typing.Union[
    str,
    tuple[str, dict[str, JSONValue]],
    TableDict,
    table.Table,
]

#
# # buckets
#


class Bucket(typing.TypedDict):
    rclone_remote: str | None
    bucket_name: str | None
    path_prefix: str | None
    provider: str | None


#
# # configuration
#


class Config(typing.TypedDict):
    version: str
    tracked_tables: list[TableDict]
    use_git: bool
    default_bucket: Bucket
