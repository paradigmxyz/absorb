from __future__ import annotations

import typing

import absorb


def get_available_range(
    dataset: absorb.TableReference,
    *,
    parameters: dict[str, typing.Any] | None = None,
) -> absorb.Coverage | None:
    table = absorb.ops.resolve_table(dataset, parameters=parameters)
    return table.get_available_range()


def get_collected_range(
    dataset: absorb.TableReference,
    *,
    parameters: dict[str, typing.Any] | None = None,
) -> absorb.Coverage | None:
    table = absorb.ops.resolve_table(dataset, parameters=parameters)
    return table.get_collected_range()
