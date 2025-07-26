from __future__ import annotations

from typing_extensions import NotRequired
import typing
import datetime
import types

import polars as pl

from . import table


# chunk formats
PrimitiveIndexType = typing.Literal[
    # temporal
    'hour',
    'day',
    'week',
    'month',
    'quarter',
    'year',
    'timestamp',
    'timestamp_range',
    # numerical
    'number',
    'number_range',
    'number_list',
    # names
    'id',
    'id_list',
]


class CustomIndexType:
    def partition_into_chunks(self, coverage: Coverage) -> list[Chunk]:
        raise NotImplementedError()

    def format_value(self, chunk: CustomChunk) -> str:
        raise NotImplementedError()


class MultiIndexType(typing.TypedDict):
    type: typing.Literal['multi']
    dims: dict[str, ScalarIndexType]


class ExplicitIndexType(typing.TypedDict):
    type: PrimitiveIndexType
    number_interval: NotRequired[int | None]
    timestamp_interval: NotRequired[datetime.timedelta | None]


ScalarIndexType = typing.Union[
    PrimitiveIndexType,
    ExplicitIndexType,
    CustomIndexType,
]

IndexType = typing.Union[ScalarIndexType, MultiIndexType]

# chunks
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
CustomChunk = typing.Any
ScalarChunk = typing.Union[PrimitiveChunk, CustomChunk]
MultiChunk = dict[str, ScalarChunk]
Chunk = typing.Union[ScalarChunk, MultiChunk]

# chunk coverage
ChunkList = typing.Sequence[Chunk]
ScalarChunkRange = tuple[ScalarChunk, ScalarChunk]
MultiChunkRange = typing.Mapping[
    str, typing.Union[ScalarChunkRange, list[ScalarChunk]]
]
Coverage = typing.Union[ChunkList, ScalarChunkRange, MultiChunkRange]


class ChunkPaths(typing.TypedDict):
    type: typing.Literal['files']
    paths: list[str]


ChunkData = typing.Union[pl.DataFrame, ChunkPaths]


def get_index_type_type(
    index_type: PrimitiveIndexType,
) -> type | types.GenericAlias | None:
    import datetime

    if isinstance(index_type, str):
        return {
            'hour': datetime.datetime,
            'day': datetime.datetime,
            'week': datetime.datetime,
            'month': datetime.datetime,
            'quarter': datetime.datetime,
            'year': datetime.datetime,
            'timestamp': datetime.datetime,
            'timestamp_range': tuple[datetime.datetime, datetime.datetime],
            'count': int,
            'count_range': tuple[int, int],
            'id': str,
            'id_list': list[str],
            None: None,
        }[index_type]
    elif isinstance(index_type, dict):
        return dict[str, typing.Any]
    else:
        raise Exception()


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


class Bucket(typing.TypedDict):
    rclone_remote: str | None
    bucket_name: str | None
    path_prefix: str | None
    provider: str | None


class Config(typing.TypedDict):
    version: str
    tracked_tables: list[TableDict]
    use_git: bool
    default_bucket: Bucket
