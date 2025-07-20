from __future__ import annotations

import typing


def validate_table_dict(metadata: typing.Any) -> list[str]:
    if not isinstance(metadata, dict):
        return ['metadata is not a dictionary']

    errors = []
    for key, value_type in {
        'source_name': str,
        'table_name': str,
        'table_class': str,
        'table_version': str,
        'parameters': dict,
    }.items():
        if key not in metadata:
            errors.append(f'missing required key: {key}')
            continue
        if not isinstance(metadata[key], value_type):
            errors.append(
                f'key {key} is not of type {value_type.__name__}, got {type(metadata[key]).__name__}'
            )

    return errors
