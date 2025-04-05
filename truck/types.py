from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import datetime
    import polars as pl


class Table:
    write_range: typing.Literal['append_only', 'overwrite']
    range_format: type
    parameter_types: dict[str, typing.Any] = {}
    index_by: typing.Literal['time', 'block', 'id']
    cadence: typing.Literal['daily', 'weekly', 'monthly', 'yearly'] | None

    @classmethod
    def get_schema(cls, context: Context) -> dict[str, type[pl.DataType]]:
        raise NotImplementedError()

    @classmethod
    def collect(cls, context: Context) -> pl.DataFrame:
        raise NotImplementedError()

    @classmethod
    async def async_collect(cls, context: Context) -> pl.DataFrame:
        raise NotImplementedError()

    # paths

    @classmethod
    def get_path(cls, context: Context) -> str:
        raise NotImplementedError()

    @classmethod
    def get_paths(cls, context: Context) -> list[str]:
        raise NotImplementedError()

    # coverage

    @classmethod
    def get_available_range(cls, context: Context) -> typing.Any:
        raise NotImplementedError()

    @classmethod
    def get_collected_range(cls, context: Context) -> typing.Any:
        raise NotImplementedError()

    @classmethod
    def get_min_collected_timestamp(cls, context: Context) -> datetime.datetime:
        raise NotImplementedError()

    @classmethod
    def get_max_collected_timestamp(cls, context: Context) -> datetime.datetime:
        raise NotImplementedError()

    @classmethod
    def get_min_available_timestamp(cls, context: Context) -> datetime.datetime:
        raise NotImplementedError()

    @classmethod
    def get_max_available_timestamp(cls, context: Context) -> datetime.datetime:
        raise NotImplementedError()

    # defaults

    @classmethod
    def get_default_context(cls) -> Context:
        raise NotImplementedError()
        if len(cls.parameter_types) == 0:
            data_range = cls.get_default_data_range()
            return {
                'parameters': {},
                'data_range': data_range,
                'overwrite': False,
            }
        else:
            raise NotImplementedError()

    @classmethod
    def get_default_data_range(cls) -> typing.Any:
        raise NotImplementedError()


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
    tracked_tables: list[TrackedTable]
