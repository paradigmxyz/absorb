from __future__ import annotations

import typing
import absorb

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import polars as pl
    import tooltime


class TableBase:
    version: str = '0.1.0'

    #
    # # structure
    #
    source: str
    write_range: typing.Literal[
        'append_only', 'overwrite_all', 'overwrite_chunks'
    ]
    index_type: absorb.IndexType

    # for ongoing datasets, time to wait before checking if new data is posted
    # can be a str duration like '1h' or a float multiple of index type
    update_latency: tooltime.Timelength | float | None = None

    # dependencies
    required_packages: list[str] = []

    #
    # # parameters
    #

    parameter_types: dict[str, type | tuple[type, ...]] = {}
    default_parameters: dict[str, absorb.JSONValue] = {}
    parameters: dict[str, typing.Any]

    #
    # # naming
    #

    # use first available template that has all parameters filled
    name_template: str | list[str] = '{class_name}'
    filename_template = '{source}__{table}__{chunk}.parquet'

    def __init__(self, parameters: dict[str, absorb.JSONValue] | None = None):
        # set parameters
        if hasattr(type(self), 'parameters'):
            raise Exception(
                'parameters should not be set at the class level, use cls.default_parameters'
            )
        if parameters is None:
            parameters = {}
        else:
            parameters = parameters.copy()

        # set default parameters
        for key, value in self.default_parameters.items():
            parameters.setdefault(key, value)

        # make sure that parameters match the parameter types
        if set(parameters.keys()) != set(self.parameter_types.keys()):
            raise Exception(
                self.name() + ': parameters must match parameter_types spec'
            )
        self.parameters = parameters

        # make sure that all required parameters are set
        required_parameters: list[str] = []
        for parameter in required_parameters:
            if not hasattr(self, parameter) or getattr(self, parameter) is None:
                raise Exception('missing table parameter: ' + str(parameter))

        # make sure that append only tables have an index type
        if self.write_range == 'append_only' and (
            not hasattr(self, 'index_type') or self.index_type is None
        ):
            raise Exception('index_type is required for append only tables')

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        raise NotImplementedError()

    @classmethod
    def full_class_name(cls) -> str:
        return cls.__module__ + '.' + cls.__qualname__

    @classmethod
    def class_name(
        cls,
        allow_generic: bool = False,
        parameters: dict[str, absorb.JSONValue] | None = None,
    ) -> str:
        # build class parameters
        if parameters is not None:
            parameters = dict(cls.default_parameters, **parameters)
        else:
            parameters = cls.default_parameters
        return absorb.ops.get_table_name(
            class_name=cls.__name__,
            template=cls.name_template,
            parameters=parameters,
            allow_generic=allow_generic,
        )

    def name(self) -> str:
        return absorb.ops.get_table_name(
            class_name=type(self).__name__,
            template=self.name_template,
            parameters=self.parameters,
        )

    # defaults

    @staticmethod
    def instantiate(
        ref: absorb.TableReference,
        *,
        parameters: dict[str, typing.Any] | None = None,
        raw_parameters: dict[str, str] | None = None,
        use_all_parameters: bool = True,
    ) -> absorb.Table:
        # reference already instantiated
        if isinstance(ref, absorb.Table):
            if parameters is not None or raw_parameters is not None:
                raise Exception('Cannot pass parameters with table instance')
            return ref

        # get class and parameters
        if isinstance(ref, dict):
            # reference is a table dict
            cls = absorb.ops.get_table_class(class_path=ref['table_class'])
            if parameters is not None or raw_parameters is not None:
                raise Exception('Cannot pass parameters with table dict')
            parameters = ref['parameters']

        elif isinstance(ref, str):
            # reference is a str
            source, table_name = ref.split('.')
            class_name = absorb.ops.names._snake_to_camel(table_name)

            # identify class and parse name parameters
            class_path = 'absorb.datasets.' + source + '.' + class_name
            cls = absorb.ops.get_table_class(class_path=class_path)
            name_parameters: dict[str, typing.Any] = {}

            # parse input parameters
            parameters = _process_input_parameters(
                parameters=parameters,
                raw_parameters=raw_parameters,
                cls=cls,
            )

            # merge name parameters into input parameters
            parameters = dict(parameters, **name_parameters)

        else:
            raise Exception()

        return cls(parameters=parameters)

    # validity

    @classmethod
    def get_missing_packages(cls) -> list[str]:
        return [
            package
            for package in cls.required_packages
            if not absorb.ops.is_package_installed(package)
        ]


def _process_input_parameters(
    parameters: dict[str, typing.Any] | None,
    raw_parameters: dict[str, str] | None,
    cls: type[TableBase],
) -> dict[str, typing.Any]:
    if parameters is None:
        parameters = {}

    # convert types of raw parameters
    if raw_parameters is not None:
        value: typing.Any
        for key, value in raw_parameters.items():
            parameter_type = cls.parameter_types[key]
            if parameter_type == str:  # noqa: E721
                pass
            elif parameter_type == int:  # noqa: E721
                value = int(value)
            elif parameter_type == list[str]:
                value = value.split(',')
            elif parameter_type == list[int]:
                value = [int(subvalue) for subvalue in value.split(',')]
            else:
                raise Exception('invalid parameter type: ' + str(parameter_type))
            parameters[key] = value

    return parameters
