from __future__ import annotations

import typing

import absorb
from . import io

if typing.TYPE_CHECKING:
    import polars as pl


def query(
    table: absorb.TableReference,
    *,
    update: bool = False,
    collect_if_missing: bool = True,
    scan_kwargs: dict[str, typing.Any] | None = None,
) -> pl.LazyFrame:
    table = absorb.Table.instantiate(table)

    # check if collected
    if not table.is_collected():
        if collect_if_missing or update:
            table.collect()
        else:
            raise Exception(
                f'Table {table.source}.{table.name()} is not collected.'
            )
    elif update:
        table.collect()

    # scan the table
    return io.scan(table, scan_kwargs=scan_kwargs)


def sql_query(
    sql: str,
    *,
    backend: typing.Literal['absorb', 'dune', 'snowflake'] = 'absorb',
) -> pl.LazyFrame:
    if backend == 'absorb':
        # create table context
        context = create_sql_context()

        # modify query to allow dots in names
        for table in context.tables():
            if '.' in table and table in sql:
                sql = sql.replace(table, '"' + table + '"')

        return context.execute(sql)  # type: ignore
    elif backend == 'dune':
        import spice

        return spice.query(sql).lazy()
    elif backend == 'snowflake':
        import garlic

        return garlic.query(sql).lazy()
    else:
        raise Exception('invalid backend: ' + backend)


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
