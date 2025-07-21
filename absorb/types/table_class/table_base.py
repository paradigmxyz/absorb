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

    @classmethod
    def parse_name_parameters(cls, table_name: str) -> dict[str, typing.Any]:
        name_templates = (
            cls.name_template
            if isinstance(cls.name_template, list)
            else [cls.name_template]
        )
        for template in name_templates:
            try:
                raw = parse_string_from_template(template, table_name)
            except absorb.NameParseError:
                continue
            return cls.convert_raw_parameter_types(raw)
        else:
            raise absorb.NameParseError('Could not parse ' + table_name)

    @classmethod
    def convert_raw_parameter_types(
        cls, raw_parameters: dict[str, str]
    ) -> dict[str, typing.Any]:
        parameters = {}
        for key, value in raw_parameters.items():
            parameter_type = cls.parameter_types[key]
            if parameter_type == str:  # noqa: E721
                converted: typing.Any = value
            elif parameter_type == int:  # noqa: E721
                converted = int(value)
            elif parameter_type == list[str]:
                converted = value.split(',')
            elif parameter_type == list[int]:
                converted = [int(subvalue) for subvalue in value.split(',')]
            else:
                raise Exception(
                    'invalid parameter type: ' + str(parameter_type)
                )
            parameters[key] = converted
        return parameters

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
            cls, parameters = parse_table_str(
                ref,
                parameters=parameters,
                raw_parameters=raw_parameters,
                use_all_parameters=use_all_parameters,
            )
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


def parse_table_str(
    ref: str,
    *,
    parameters: dict[str, typing.Any] | None = None,
    raw_parameters: dict[str, str] | None = None,
    use_all_parameters: bool = True,
) -> tuple[type[absorb.Table], dict[str, typing.Any]]:
    """
    Parses a table string of the form 'source.table_name' and returns the class
    and parameters.
    """
    source, table_name = ref.split('.')

    # identify class and parse name parameters
    try:
        class_name = absorb.ops.names._snake_to_camel(table_name)
        class_path = 'absorb.datasets.' + source + '.' + class_name
        cls = absorb.ops.get_table_class(class_path=class_path)
        name_parameters: dict[str, typing.Any] = {}
    except AttributeError:
        for cls in absorb.ops.get_source_table_classes(source):
            try:
                name_parameters = cls.parse_name_parameters(table_name)
                break
            except absorb.NameParseError:
                continue
        else:
            raise Exception('Could not find table class for: ' + source)

    # parse input parameters
    if parameters is None:
        parameters = {}
    if raw_parameters is not None:
        parameters.update(cls.convert_raw_parameter_types(raw_parameters))

    # merge name parameters into input parameters
    parameters = dict(parameters, **name_parameters)

    # only use subset relevant to Table class
    for key, value in parameters.items():
        if key not in cls.parameter_types:
            if not use_all_parameters:
                del parameters[key]
            else:
                raise Exception(
                    'Invalid parameter ' + key + ' for ' + cls.full_class_name()
                )

    return cls, parameters


def parse_string_from_template(template: str, string: str) -> dict[str, str]:
    import re

    # Find all placeholders in the template
    placeholder_pattern = r'\{([^}]+)\}'
    placeholders = re.findall(placeholder_pattern, template)

    if not placeholders:
        raise absorb.NameParseError('No placeholders found in template')

    # Escape special regex characters in the template, but preserve placeholders
    regex_pattern = re.escape(template)

    # Replace escaped placeholders with capture groups
    # Use non-greedy matching by default to handle multiple placeholders better
    for placeholder in placeholders:
        escaped_placeholder = re.escape('{' + placeholder + '}')
        # Create a named capture group for each placeholder
        # Use a non-greedy match that stops at the next literal character
        regex_pattern = regex_pattern.replace(
            escaped_placeholder, f'(?P<{placeholder}>.*?)'
        )

    # For the last placeholder, we might want greedy matching
    # Adjust the pattern to make the last group greedy if it's at the end
    if regex_pattern.endswith('.*?)'):
        regex_pattern = regex_pattern[:-4] + '.*)'

    # Add anchors to ensure full string match
    regex_pattern = f'^{regex_pattern}$'

    try:
        match = re.match(regex_pattern, string)
        if not match:
            raise absorb.NameParseError(
                f"String '{string}' does not match template '{template}'"
            )

        return match.groupdict()
    except re.error as e:
        raise absorb.NameParseError(f'Invalid regex pattern: {e}')
