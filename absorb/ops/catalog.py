from __future__ import annotations

import os
import typing
import absorb

from . import names

if typing.TYPE_CHECKING:
    import types
    import polars as pl


def get_source_module(source: str) -> types.ModuleType:
    import importlib

    return importlib.import_module(
        'absorb.catalog.' + names._camel_to_snake(source)
    )


def get_table_class(
    *,
    source: str | None = None,
    table_name: str | None = None,
    class_path: str | None = None,
) -> type[absorb.Table]:
    if class_path is not None:
        import importlib

        module_name, class_name = class_path.rsplit('.', maxsplit=1)
        module = importlib.import_module(module_name)
        return getattr(module, class_name)  # type: ignore
    elif source is not None and table_name is not None:
        return getattr(  # type: ignore
            get_source_module(source), names._snake_to_camel(table_name)
        )
    else:
        raise Exception(
            'either specify class_path or both source and table_name'
        )


def get_sources(*, snake: bool = True) -> list[str]:
    import absorb.catalog

    sources = [
        filename.rsplit('.py', maxsplit=1)[0]
        for filename in os.listdir(absorb.catalog.__path__[0])
        if not filename.startswith('__')
    ]

    if not snake:
        sources = [names._snake_to_camel(source) for source in sources]

    return sources


def get_source_table_classes(source: str) -> list[type[absorb.Table]]:
    module = get_source_module(source)
    if hasattr(module, 'get_tables'):
        return module.get_tables()  # type: ignore
    else:
        return [
            value
            for key, value in vars(module).items()
            if isinstance(value, type) and issubclass(value, absorb.Table)
        ]


def get_table_classes() -> list[type[absorb.Table]]:
    return [
        table_class
        for source in get_sources()
        for table_class in get_source_table_classes(source)
    ]
