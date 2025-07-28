from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


class Treasuries(absorb.Table):
    source = 'tic'
    description = 'Holdings of US Treasury securities per each country'
    url = 'https://home.treasury.gov/data/treasury-international-capital-tic-system'
    write_range = 'overwrite_all'
    row_precision = 'month'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'country': pl.String,
            'for_treas_pos': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return get_post_2019_holdings()

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        holdings = get_post_2019_holdings()
        max_timestamp = typing.cast(
            datetime.datetime, holdings['timestamp'].max()
        )
        return (datetime.datetime(2011, 9, 1), max_timestamp)


def get_post_2019_holdings() -> pl.DataFrame:
    import io
    import requests
    import polars as pl

    # TODO: get pre-2019 data from https://treasury.gov/resource-center/data-chart-center/tic/Documents/slt3d_globl.csv  # noqa

    url = 'https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/slt_table3.txt'  # noqa
    response = requests.get(url)
    end_index = response.text.index('Notes:	')
    file = io.StringIO(response.text[:end_index])

    schema = {
        'country': pl.String,
        'country_code': pl.String,
        'timestamp': pl.String,
        'for_treas_pos': pl.String,
        'for_treas_net': pl.String,
        'for_lt_treas_pos': pl.String,
        'for_lt_treas_net': pl.String,
        'for_lt_treas_valchg': pl.String,
        'for_st_treas_pos': pl.String,
        'for_st_treas_net': pl.String,
    }

    holders = (
        pl.read_csv(
            file,
            separator='\t',
            schema=schema,
            skip_rows=10,
            truncate_ragged_lines=True,
        )[:-1]
        .with_columns(pl.col('*').replace('n.a.', None))
        .with_columns(
            country_code=pl.col.country_code.cast(int),
            timestamp=pl.col.timestamp.str.to_date('%Y-%m').cast(
                pl.Datetime('us', 'UTC')
            ),
            for_treas_pos=pl.col.for_treas_pos.cast(float),
            for_treas_net=pl.col.for_treas_net.cast(float),
            for_lt_treas_pos=pl.col.for_lt_treas_pos.cast(float),
            for_lt_treas_net=pl.col.for_lt_treas_net.cast(float),
            for_lt_treas_valchg=pl.col.for_lt_treas_valchg.cast(float),
            for_st_treas_pos=pl.col.for_st_treas_pos.cast(float),
            for_st_treas_net=pl.col.for_st_treas_net.cast(float),
        )
        .select(
            'timestamp',
            'country',
            'for_treas_pos',
        )
        .sort('country', 'timestamp')
    )

    return holders
