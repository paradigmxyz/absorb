from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


class Query(absorb.Table):
    source = 'snowflake'
    write_range = 'overwrite_all'
    range_format = 'per_hour'
    parameters = {'name': str, 'sql': str}
    required_packages = ['garlic >= 1.1']

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        raise NotImplementedError()

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        import garlic

        sql = self.parameters['sql']
        return garlic.query(sql)

    def get_available_range(self) -> typing.Any:
        raise NotImplementedError()
