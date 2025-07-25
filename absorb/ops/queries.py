from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


def sql_query(sql: str) -> pl.LazyFrame:
    # create table context
    context = create_sql_context()

    # modify query to allow dots in names
    for table in context.tables():
        if '.' in table and table in sql:
            sql = sql.replace(table, '"' + table + '"')

    return context.execute(sql)  # type: ignore


def create_sql_context(
    *,
    tracked_tables: bool = True,
    collected_tables: bool = True,
) -> pl.SQLContext[typing.Any]:
    import polars as pl

    # decide which tables to include
    all_tables = []
    if tracked_tables:
        all_tables += absorb.ops.get_tracked_tables()
    if collected_tables:
        all_tables += absorb.ops.get_collected_tables()

    # index tables by full name
    tables_by_name = {}
    for table_dict in all_tables:
        name = table_dict['source_name'] + '.' + table_dict['table_name']
        if name not in tables_by_name:
            tables_by_name[name] = absorb.scan(table_dict)

    # create context
    return pl.SQLContext(**tables_by_name)  # type: ignore
