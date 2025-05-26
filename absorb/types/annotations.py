from __future__ import annotations

from typing_extensions import NotRequired
import typing
import datetime
import types

from . import table_class


# chunk formats
PrimitiveChunkFormat = typing.Literal[
    'all',
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
    'name',
    'name_list',
]


class CustomChunkFormat:
    def partition_into_chunks(self, coverage: Coverage) -> list[Chunk]:
        raise NotImplementedError()

    def format_value(self, chunk: CustomChunk) -> str:
        raise NotImplementedError()


class MultiChunkFormat(typing.TypedDict):
    type: typing.Literal['multi']
    dims: dict[str, ScalarChunkFormat]


class ExplicitChunkFormat(typing.TypedDict):
    type: PrimitiveChunkFormat
    number_interval: NotRequired[int | None]
    timestamp_range: NotRequired[datetime.timedelta | None]


ScalarChunkFormat = typing.Union[
    PrimitiveChunkFormat,
    ExplicitChunkFormat,
    CustomChunkFormat,
]

ChunkFormat = typing.Union[ScalarChunkFormat, MultiChunkFormat]

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


def get_chunk_format_type(
    chunk_format: ChunkFormat,
) -> type | types.GenericAlias | None:
    import datetime

    if isinstance(chunk_format, str):
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
            'name': str,
            'name_list': list[str],
            None: None,
        }[chunk_format]
    elif isinstance(chunk_format, dict):
        return dict[str, typing.Any]
    else:
        raise Exception()


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
