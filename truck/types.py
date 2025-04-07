from __future__ import annotations

import typing
import truck

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import datetime
    import polars as pl


class Table:
    source: str
    write_range: typing.Literal['append_only', 'overwrite']
    range_format: type
    index_by: typing.Literal['time', 'block', 'id']
    cadence: typing.Literal['daily', 'weekly', 'monthly', 'yearly'] | None
    parameter_types: dict[str, typing.Any] = {}
    filename_template = '{source}__{table}__{data_range}.parquet'

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

    def get_path(self, data_range: typing.Any) -> str:
        format_params = self.parameters.copy()
        if self.source is not None:
            format_params['source'] = self.source
        format_params['table'] = truck.ops.names._camel_to_snake(
            str(type(self))
        )
        if '{data_range}' in self.filename_template:
            format_params['data_range'] = self.format_data_range(data_range)
        return self.filename_template.format(**format_params)

    # def get_paths(self, *args, **kwargs) -> list[str]:
    #     raise NotImplementedError()

    def parse_path(self, path: str) -> dict[str, typing.Any]:
        import os

        keys = os.path.splitext(self.filename_template)[0].split('__')
        values = os.path.splitext(path)[0].split('__')
        return {k[1:-1]: v for k, v in zip(keys, values)}

    def format_data_range(self, data_range: typing.Any) -> str:
        import datetime

        if isinstance(data_range, str):
            return data_range
        elif isinstance(data_range, datetime.datetime):
            return data_range.strftime('%Y-%m-%d')
        elif isinstance(data_range, list) and len(data_range) == 2:
            return (  # type: ignore
                data_range[0].strftime('%Y-%m-%d')
                + '_to_'
                + data_range[1].strftime('%Y-%m-%d')
            )
        else:
            raise NotImplementedError()

    def parse_data_range(self, as_str: str) -> typing.Any:
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
