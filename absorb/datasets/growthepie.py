# https://docs.growthepie.xyz/api

from __future__ import annotations

import absorb

import typing

if typing.TYPE_CHECKING:
    import polars as pl


class Metrics(absorb.Table):
    source = 'growthepie'
    write_range = 'overwrite_all'
    index_type = 'day'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        df = self.collect_chunk(None)
        if df is not None:
            return dict(df.schema)
        else:
            raise Exception('data not found')

    def collect_chunk(self, chunk: absorb.Chunk) -> pl.DataFrame | None:
        import requests
        import polars as pl

        url = 'https://api.growthepie.xyz/v1/fundamentals_full.json'
        response = requests.get(url)
        data = response.json()
        return (
            pl.DataFrame(data)
            .with_columns(date=pl.col.date.str.to_date().cast(pl.Datetime))
            .rename({'origin_key': 'network'})
            .pivot(on='metric_key', index=['date', 'network'], values='value')
            .sort('date', 'network')
        )

    def get_available_range(self) -> absorb.Coverage:
        import datetime
        import requests
        import polars as pl

        first = datetime.datetime(year=2021, month=6, day=1)

        url = 'https://api.growthepie.xyz/v1/fundamentals.json'
        response = requests.get(url)
        data = response.json()
        last_str: str = pl.DataFrame(data)['date'].max()  # type: ignore
        last = datetime.datetime.strptime(last_str, '%Y-%m-%d')

        return (first, last)
