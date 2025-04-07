from __future__ import annotations

import typing
import truck

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import datetime
    import polars as pl


class Table:
    write_range: typing.Literal['append_only', 'overwrite']
    range_format: type
    index_by: typing.Literal['time', 'block', 'id']
    cadence: typing.Literal['daily', 'weekly', 'monthly', 'yearly'] | None
    parameter_types: dict[str, typing.Any] = {}

    def __init__(self, parameters: dict[str, typing.Any] | None = None):
        if parameters is None:
            self.parameters = {}
        else:
            self.parameters = parameters

    def get_schema(self) -> dict[str, type[pl.DataType]]:
        raise NotImplementedError()

    def collect(self, data_range: typing.Any) -> pl.DataFrame:
        raise NotImplementedError()

    async def async_collect(self, data_range: typing.Any) -> pl.DataFrame:
        raise NotImplementedError()

    @classmethod
    def class_name(cls, snake: bool = True) -> str:
        if snake:
            return truck.ops.names._camel_to_snake(cls.__name__)
        else:
            return cls.__name__

    def name(self, snake: bool = True) -> str:
        if snake:
            return truck.ops.names._camel_to_snake(type(self).__name__)
        else:
            return type(self).__name__

    # paths

    def get_path(self) -> str:
        raise NotImplementedError()

    def get_paths(self) -> list[str]:
        raise NotImplementedError()

    # coverage

    def get_available_range(self) -> typing.Any:
        return None

    def get_collected_range(self) -> typing.Any:
        raise NotImplementedError()

    def get_min_collected_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()

    def get_max_collected_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()

    def get_min_available_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()

    def get_max_available_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()

    # defaults

    def get_default_data_range(self) -> typing.Any:
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
    version: str
    tracked_tables: list[TrackedTable]
