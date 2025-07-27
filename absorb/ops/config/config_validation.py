from __future__ import annotations

import typing

import absorb
from .. import paths

if typing.TYPE_CHECKING:
    import typing_extensions


def validate_config(
    config: typing.Any,
) -> typing_extensions.TypeGuard[absorb.Config]:
    return (
        isinstance(config, dict)
        and {'tracked_tables'}.issubset(set(config.keys()))
        and isinstance(config['tracked_tables'], list)
    )
