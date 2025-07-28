from __future__ import annotations

import typing

import absorb
from . import table_coverage


class TableCreate(table_coverage.TableCoverage):
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
                self.full_name_classmethod()
                + ': parameters must match parameter_types spec'
            )
        self.parameters = parameters

        # make sure that all required parameters are set
        required_parameters: list[str] = []
        for parameter in required_parameters:
            if not hasattr(self, parameter) or getattr(self, parameter) is None:
                raise Exception('missing table parameter: ' + str(parameter))

        # make sure that append only tables have an index type
        if self.write_range == 'append_only' and self.get_index_type() is None:
            raise Exception('index_type is required for append only tables')

        # make sure that table name is valid
        if not absorb.ops.is_valid_name(self.name()):
            raise Exception(f'table name "{self.name()}" is not valid')

        # make sure that source name is valid
        if not absorb.ops.is_valid_name(self.source):
            raise Exception(f'source name "{self.source}" is not valid')

    @staticmethod
    def instantiate(
        ref: absorb.TableReference,
        *,
        raw_parameters: dict[str, str] | None = None,
        use_all_parameters: bool = True,
        use_config: bool = True,
    ) -> absorb.Table:
        if isinstance(ref, absorb.Table):
            # reference already instantiated
            if raw_parameters is not None:
                raise Exception('Cannot pass parameters with table instance')
            return ref
        elif isinstance(ref, dict):
            # reference is a table dict
            cls = absorb.ops.get_table_class(class_path=ref['table_class'])
            if raw_parameters is not None:
                raise Exception('Cannot pass parameters with table dict')
            parameters = ref['parameters']
        elif isinstance(ref, str):
            # reference is a str
            cls, parameters = absorb.ops.parse_table_str(
                ref,
                raw_parameters=raw_parameters,
                use_all_parameters=use_all_parameters,
                use_config=use_config,
            )
        elif isinstance(ref, tuple):
            tuple_name, tuple_parameters = ref
            cls, parameters = absorb.ops.parse_table_str(
                tuple_name,
                parameters=tuple_parameters,
                raw_parameters=raw_parameters,
                use_all_parameters=use_all_parameters,
                use_config=use_config,
            )
        else:
            raise Exception()

        return cls(parameters=parameters)
