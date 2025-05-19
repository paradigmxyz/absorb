from __future__ import annotations

import typing

import truck

if typing.TYPE_CHECKING:
    import polars as pl
    from google.cloud import bigquery


class Query(truck.Table):
    source = 'bigquery'
    write_range = 'overwrite_all'
    parameters = {'sql': str, 'name': str}

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        raise NotImplementedError()

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        return query(self.parameters['sql'])

    def get_available_range(self) -> typing.Any:
        raise NotImplementedError()


def get_client() -> bigquery.Client:
    import google.auth
    from google.cloud import bigquery

    try:
        return bigquery.Client()
    except google.auth.exceptions.DefaultCredentialsError as e:
        print(
            'credentials not set up. create a google cloud service account and set the GOOGLE_APPLICATION_CREDENTIALS env variable to the json file of the credentials'  # noqa
        )
        raise e


def query(sql: str) -> pl.DataFrame:
    import pyarrow as pa  # type: ignore
    import pyarrow.compute as pc  # type: ignore

    client = get_client()

    # execute query
    query_job = client.query(sql)

    # retrieve query results as arrow
    results = query_job.result()
    arrow = results.to_arrow()

    # drop nested types
    keep_columns = [
        field.name
        for field in arrow.schema
        if not (
            isinstance(field.type, pa.StructType)
            or isinstance(field.type, pa.ListType)
        )
    ]
    arrow = arrow.select(keep_columns)

    # convert decimal columns to float
    decimal_cols = [
        field.name
        for field in arrow.schema
        if isinstance(field.type, pa.Decimal128Type)
        or isinstance(field.type, pa.Decimal256Type)
    ]
    new_columns = []
    for col_name in arrow.column_names:
        col = arrow[col_name]
        if col_name in decimal_cols:
            casted_col = pc.cast(col, pa.float64())
            new_columns.append(casted_col)
        else:
            new_columns.append(col)
    arrow = pa.Table.from_arrays(new_columns, names=arrow.column_names)

    # convert from arrow to polars
    df: pl.DataFrame = pl.from_arrow(arrow)  # type: ignore

    return df
