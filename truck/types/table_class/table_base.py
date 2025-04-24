from __future__ import annotations

import typing
import truck

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import polars as pl


class TableBase:
    source: str
    write_range: typing.Literal[
        'append_only', 'overwrite_all', 'overwrite_chunks'
    ]
    range_format: truck.types.annotations.RangeFormat
    index_by: typing.Literal['time', 'block', 'id']
    cadence: typing.Literal['daily', 'weekly', 'monthly', 'yearly'] | None
    parameter_types: dict[str, typing.Any] = {}
    parameters: dict[str, typing.Any] = {}
    filename_template = '{source}__{table}__{data_range}.parquet'

    def __init__(self, parameters: dict[str, typing.Any] | None = None):
        if parameters is not None:
            if set(parameters.keys()) != set(self.parameter_types.keys()):
                raise Exception('parameters must match parameter_types spec')
            self.parameters = parameters

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
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

    # defaults

    def get_default_data_range(self) -> typing.Any:
        raise NotImplementedError()

    @staticmethod
    def instantiate(dataset: truck.TrackedTable) -> truck.Table:
        import importlib

        module_name, class_name = dataset['table_class'].rsplit('.', maxsplit=1)
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        return cls(parameters=dataset['parameters'])  # type: ignore
