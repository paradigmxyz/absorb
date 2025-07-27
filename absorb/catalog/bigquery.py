from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl
    from google.cloud import bigquery


class Query(absorb.Table):
    source = 'bigquery'
    description = 'Query data from Google BigQuery'
    url = 'https://cloud.google.com/bigquery?hl=en'
    write_range = 'overwrite_all'
    parameter_types = {'sql': str, 'name': str}
    required_packages = [
        'google-cloud-bigquery >= 3.32.0',
        'google-cloud-bigquery-storage >= 2.31.0',
    ]

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        raise NotImplementedError()

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return query(self.parameters['sql'])

    def get_available_range(self) -> absorb.Coverage:
        raise NotImplementedError()


def get_client() -> bigquery.Client:
    import google.auth.exceptions
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
