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
    description: str | None = None
    write_range: typing.Literal[
        'append_only', 'overwrite_all', 'overwrite_chunks'
    ]
    index_type: absorb.IndexType

    # for ongoing datasets, time to wait before checking if new data is posted
    # can be a str duration like '1h' or a float multiple of index type
    update_latency: tooltime.Timelength | float | None = None

    # dependencies
    required_packages: list[str] = []
    required_credentials: list[str] = []

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

    #
    # # methods
    #

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        raise NotImplementedError()

    @classmethod
    def get_missing_packages(cls) -> list[str]:
        return [
            package
            for package in cls.required_packages
            if not absorb.ops.is_package_installed(package)
        ]
