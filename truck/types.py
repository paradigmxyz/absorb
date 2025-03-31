from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')


class Table:
    write_range: typing.Literal['append_only', 'overwrite']
    range_format: T
    parameter_types: dict[str, typing.Any] = {}

    @classmethod
    def get_schema(cls, context: Context) -> pl.DataFrame:
        raise NotImplementedError()

    @classmethod
    def get_available_range(cls, context: Context) -> T:
        raise NotImplementedError()

    @classmethod
    def collect(cls, context: Context) -> pl.DataFrame:
        raise NotImplementedError()

    @classmethod
    async def async_collect(cls, context: Context) -> pl.DataFrame:
        raise NotImplementedError()


class Context(typing.TypedDict):
    parameters: dict[str, typing.Any]
    data_range: typing.Any


class TrackedTable(typing.TypedDict):
    source_name: str
    table_name: str
    table_class: TableClassReference
    parameters: dict[str, typing.Any]


class TruckConfig(typing.TypedDict):
    tracked_tables: list[TrackedTable]
