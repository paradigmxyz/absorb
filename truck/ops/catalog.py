from __future__ import annotations

import typing
import truck

if typing.TYPE_CHECKING:
    import types


def get_source_module(data_source: str) -> types.ModuleType:
    import importlib

    return importlib.import_module('truck.datasets.' + _camel_to_snake(dataset))


def get_table_class(source: str, table_name: str) -> type[truck.Table]:
    return getattr(get_source_module(module), _snake_to_camel(table_name))


def get_sources() -> list[str]:
    import truck.datasets

    return [
        filename.rsplit('.py', maxsplit=1)[0]
        for filename in os.listdir(truck.datasets.__path__[0])
        if not filename.startswith('__')
    ]


def get_source_tables(source: str) -> list[str]:
    module = get_source_module(source)
    if hasattr(module, 'get_tables'):
        return module.get_tables()
    else:
        return [
            value
            for key, value in vars(module).items()
            if issubclass(value, truck.Table)
        ]


def _camel_to_snake(name: str) -> str:
    result = []
    for i, char in enumerate(name):
        if char.isupper():
            if i != 0:
                result.append('_')
            result.append(char.lower())
        else:
            result.append(char)
    return ''.join(result)


def _snake_to_camel(name: str) -> str:
    result = []
    capitalize_next = False

    for i, char in enumerate(name):
        if char == '_':
            capitalize_next = True
        elif capitalize_next:
            result.append(char.upper())
            capitalize_next = False
        else:
            # Keep first letter lowercase for camelCase convention
            if i == 0:
                result.append(char.lower())
            else:
                result.append(char)

    return ''.join(result)
