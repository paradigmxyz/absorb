from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


class Query(absorb.Table):
    source = 'snowflake'
    write_range = 'overwrite_all'
    description = 'Snowflake SQL query'
    url = 'https://www.snowflake.com/en/'
    # parameter_types = {'name': str, 'sql': str}
    # parameter_types = {'name': str}
    required_packages = ['paradigm_garlic >= 0.1.2']
    sql: str
    # name_template = 'snowflake_query_{name}'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import garlic

        sql = (
            'WITH cte AS ('
            + self.sql.rstrip(';')
            + '), SELECT * FROM cte LIMIT 0'
        )
        result = garlic.query(sql)
        return dict(result.schema)

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import garlic

        if not hasattr(self, 'sql'):
            raise ValueError('SQL query must be defined in the subclass.')

        return garlic.query(self.sql)
