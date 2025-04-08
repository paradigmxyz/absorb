from __future__ import annotations

import typing
import truck

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import datetime
    import polars as pl


RangeFormat = typing.Literal[
    'date', 'date_range', 'named_range', 'block_range', 'id_range', None
]


class Table:
    source: str
    write_range: typing.Literal['append_only', 'overwrite']
    range_format: RangeFormat
    index_by: typing.Literal['time', 'block', 'id']
    cadence: typing.Literal['daily', 'weekly', 'monthly', 'yearly'] | None
    parameter_types: dict[str, typing.Any] = {}
    filename_template = '{source}__{table}__{data_range}.parquet'

    def __init__(self, parameters: dict[str, typing.Any] | None = None):
        if parameters is None:
            self.parameters = {}
        else:
            if set(parameters.keys()) != set(self.parameter_types.keys()):
                raise Exception('parameters must match parameter_types spec')
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

    def get_dir_path(self, warn: bool = True) -> str:
        return truck.ops.paths.get_table_dir(
            source=self.source,
            table=truck.ops.names._camel_to_snake(str(type(self))),
            warn=warn,
        )

    def get_glob(self, warn: bool = True) -> str:
        return self.get_file_path(glob=True, warn=warn)

    def get_file_path(
        self,
        data_range: typing.Any | None = None,
        glob: bool = False,
        warn: bool = True,
    ) -> str:
        return truck.ops.paths.get_table_filepath(
            data_range=data_range,
            range_format=self.range_format,
            filename_template=self.filename_template,
            table=truck.ops.names._camel_to_snake(str(type(self))),
            source=self.source,
            parameters=self.parameters,
            glob=glob,
            warn=warn,
        )

    def get_file_paths(
        self, data_ranges: typing.Any, warn: bool = True
    ) -> list[str]:
        return truck.ops.paths.get_table_filepaths(
            data_ranges=data_ranges,
            range_format=self.range_format,
            filename_template=self.filename_template,
            table=truck.ops.names._camel_to_snake(str(type(self))),
            source=self.source,
            parameters=self.parameters,
            warn=warn,
        )

    def get_file_name(
        self, data_range: typing.Any, *, glob: bool = False
    ) -> str:
        return truck.ops.paths.get_table_filename(
            data_range=data_range,
            range_format=self.range_format,
            filename_template=self.filename_template,
            table=truck.ops.names._camel_to_snake(str(type(self))),
            source=self.source,
            parameters=self.parameters,
            glob=glob,
        )

    def parse_file_path(self, path: str) -> dict[str, typing.Any]:
        return truck.ops.paths.parse_file_path(
            path=path,
            filename_template=self.filename_template,
            range_format=self.range_format,
        )

    # coverage

    def get_available_range(self) -> typing.Any:
        return None

    def get_collected_range(self) -> typing.Any:
        import os
        import glob

        dir_path = self.get_dir_path()
        if not os.path.isdir(dir_path):
            return None

        glob_str = self.get_glob()
        if self.is_range_sortable():
            files = sorted(glob.glob(glob_str))
            start = self.parse_file_path(files[0])['data_range']
            end = self.parse_file_path(files[-1])['data_range']
            return [start, end]
        else:
            raise Exception()

    @classmethod
    def is_range_sortable(cls) -> bool:
        return cls.range_format is not None

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


def instantiate(dataset: truck.TrackedTable) -> truck.Table:
    import importlib

    module_name, class_name = dataset['table_class'].rsplit('.', maxsplit=1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    return cls(parameters=dataset['parameters'])  # type: ignore
