from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


class Query(absorb.Table):
    source = 'snowflake'
    write_range = 'overwrite_all'
    parameters = {'name': str, 'sql': str}
    required_packages = ['garlic >= 1.1']

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        raise NotImplementedError()

    def collect_chunk(self, chunk: absorb.Chunk) -> pl.DataFrame | None:
        import garlic

        sql = self.parameters['sql']
        return garlic.query(sql)

    def get_available_range(self) -> absorb.Coverage:
        raise NotImplementedError()
