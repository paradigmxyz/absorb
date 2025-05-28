from __future__ import annotations

import typing
import absorb

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import polars as pl


class TableBase:
    # structure
    source: str
    write_range: typing.Literal[
        'append_only', 'overwrite_all', 'overwrite_chunks'
    ]
    index_type: absorb.IndexType

    # dependencies
    required_packages: list[str] = []

    # parameters
    parameter_types: dict[str, typing.Any] = {}
    default_parameters: dict[str, typing.Any] = {}
    parameters: dict[str, typing.Any] = {}
    static_parameters: list[str] = []

    # naming
    name_template: absorb.NameTemplate = {}
    filename_template = '{source}__{table}__{chunk}.parquet'

    def __init__(self, parameters: dict[str, typing.Any] | None = None):
        # set parameters
        if parameters is None:
            parameters = {}
        else:
            parameters = parameters.copy()
        for parameter in parameters.keys():
            if parameter in self.static_parameters:
                raise Exception('cannot change parameter: ' + parameter)
        parameters = dict(self.parameters, **parameters)
        for key, value in self.default_parameters.items():
            parameters.setdefault(key, value)
        if set(parameters.keys()) != set(self.parameter_types.keys()):
            raise Exception(
                self.name() + ': parameters must match parameter_types spec'
            )
        self.parameters = parameters

        required_parameters: list[str] = []
        for parameter in required_parameters:
            if not hasattr(self, parameter) or getattr(self, parameter) is None:
                raise Exception('missing table parameter: ' + str(parameter))

        if self.write_range == 'append_only' and (
            not hasattr(self, 'index_type') or self.index_type is None
        ):
            raise Exception('index_type is required for append only tables')

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        raise NotImplementedError()

    @classmethod
    def class_name(
        cls,
        allow_generic: bool = False,
        parameters: dict[str, typing.Any] | None = None,
    ) -> str:
        if parameters is not None:
            parameters = dict(
                cls.default_parameters, **cls.parameters, **parameters
            )
        else:
            parameters = dict(cls.default_parameters, **cls.parameters)
        return absorb.ops.get_table_name(
            base_name=cls.__name__,
            name_template=cls.name_template,
            parameter_types=cls.parameter_types,
            parameters=parameters,
            default_parameters=cls.default_parameters,
            static_parameters=cls.static_parameters,
            allow_generic=allow_generic,
        )

    def name(self) -> str:
        return absorb.ops.get_table_name(
            base_name=type(self).__name__,
            name_template=self.name_template,
            parameter_types=self.parameter_types,
            parameters=self.parameters,
            default_parameters=self.default_parameters,
            static_parameters=self.static_parameters,
        )

    # defaults

    @staticmethod
    def instantiate(dataset: absorb.TrackedTable) -> absorb.Table:
        import importlib

        module_name, class_name = dataset['table_class'].rsplit('.', maxsplit=1)
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        return cls(parameters=dataset['parameters'])  # type: ignore
