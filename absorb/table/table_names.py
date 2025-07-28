from __future__ import annotations

import typing

import absorb

from . import table_properties


class TableNames(table_properties.TableProperties):
    @classmethod
    def full_class_name(cls) -> str:
        return cls.__module__ + '.' + cls.__qualname__

    @classmethod
    def name_classmethod(
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

    def full_name_classmethod(
        cls,
        allow_generic: bool = False,
        parameters: dict[str, absorb.JSONValue] | None = None,
    ) -> str:
        return (
            cls.source
            + '.'
            + cls.name_classmethod(
                allow_generic=allow_generic, parameters=parameters
            )
        )

    def full_name(self) -> str:
        return self.source + '.' + self.name()

    @classmethod
    def parse_name_parameters(cls, table_name: str) -> dict[str, typing.Any]:
        name_templates = (
            cls.name_template
            if isinstance(cls.name_template, list)
            else [cls.name_template]
        )
        for template in name_templates:
            template = template.replace(
                '{class_name}', absorb.ops.names._camel_to_snake(cls.__name__)
            )
            try:
                raw = absorb.ops.parse_string_from_template(
                    template, table_name
                )
            except absorb.NameParseError:
                continue
            return absorb.ops.convert_raw_parameter_types(
                raw, cls.parameter_types
            )
        else:
            raise absorb.NameParseError('Could not parse ' + table_name)
