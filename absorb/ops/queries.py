from __future__ import annotations

import typing

import absorb
from . import io

if typing.TYPE_CHECKING:
    import polars as pl


@typing.overload
def query(
    table: absorb.TableReference,
    *,
    update: bool = False,
    collect_if_missing: bool = True,
    scan_kwargs: dict[str, typing.Any] | None = None,
    bucket: bool | absorb.Bucket = False,
    lazy: typing.Literal[False] = False,
) -> pl.DataFrame: ...


@typing.overload
def query(
    table: absorb.TableReference,
    *,
    update: bool = False,
    collect_if_missing: bool = True,
    scan_kwargs: dict[str, typing.Any] | None = None,
    bucket: bool | absorb.Bucket = False,
    lazy: typing.Literal[True],
) -> pl.LazyFrame: ...


def query(
    table: absorb.TableReference,
    *,
    update: bool = False,
    collect_if_missing: bool = True,
    scan_kwargs: dict[str, typing.Any] | None = None,
    bucket: bool | absorb.Bucket = False,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    lf = _query_lazy(
        table=table,
        update=update,
        collect_if_missing=collect_if_missing,
        scan_kwargs=scan_kwargs,
        bucket=bucket,
    )

    if lazy:
        return lf
    else:
        return lf.collect()


def _query_lazy(
    table: absorb.TableReference,
    *,
    update: bool = False,
    collect_if_missing: bool = True,
    scan_kwargs: dict[str, typing.Any] | None = None,
    bucket: bool | absorb.Bucket = False,
) -> pl.LazyFrame:
    if bucket:
        if isinstance(bucket, bool):
            bucket = absorb.ops.get_default_bucket()
        if update:
            raise Exception('Cannot auto update bucketed table')
        return absorb.ops.scan_bucket(
            table,
            bucket=bucket,
            scan_kwargs=scan_kwargs,
        )

    else:
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


@typing.overload
def sql_query(
    sql: str,
    *,
    backend: typing.Literal['absorb', 'dune', 'snowflake'] = 'absorb',
    lazy: typing.Literal[False] = False,
) -> pl.LazyFrame: ...


@typing.overload
def sql_query(
    sql: str,
    *,
    backend: typing.Literal['absorb', 'dune', 'snowflake'] = 'absorb',
    lazy: typing.Literal[True],
) -> pl.DataFrame: ...


def sql_query(
    sql: str,
    *,
    backend: typing.Literal['absorb', 'dune', 'snowflake'] = 'absorb',
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    if backend == 'absorb':
        # create table context
        context = create_sql_context()

        # modify query to allow dots in names
        for table in context.tables():
            if '.' in table and table in sql:
                sql = sql.replace(table, '"' + table + '"')

        lf: pl.LazyFrame = context.execute(sql)  # type: ignore
        if lazy:
            return lf
        else:
            return lf.collect()
    elif backend == 'dune':
        import spice

        df = spice.query(sql)
        if lazy:
            return df.lazy()
        else:
            return df
    elif backend == 'snowflake':
        import garlic

        df = garlic.query(sql)
        if lazy:
            return df.lazy()
        else:
            return df
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
            tables_by_name[name] = absorb.ops.scan(table_dict)

    # create context
    return pl.SQLContext(**tables_by_name)  # type: ignore
