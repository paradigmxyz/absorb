# https://docs.growthepie.xyz/api

from __future__ import annotations

import truck

import typing

if typing.TYPE_CHECKING:
    import polars as pl


class Fundamentals(truck.Table):
    source = 'growthepie'
    write_range = 'overwrite'
    range_format = 'date_range'

    def get_schema(self) -> dict[str, pl.Datatype]:
        return dict(self.collect(None).schema)

    def collect(self, data_range: typing.Any) -> pl.DataFrame:
        import requests

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

    def get_available_range(self) -> typing.Any:
        import datetime
        import requests

        first = datetime.datetime(year=2021, month=6, day=1)

        url = 'https://api.growthepie.xyz/v1/fundamentals.json'
        response = requests.get(url)
        data = response.json()
        last_str: str = pl.DataFrame(data)['date'].max()  # type: ignore
        last = datetime.datetime.strptime(last_str, '%Y-%m-%d')

        return [first, last]
