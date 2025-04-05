# https://docs.growthepie.xyz/api

from __future__ import annotations

import truck

import typing

if typing.TYPE_CHECKING:
    import polars as pl


class Fundamentals(truck.Table):
    write_range = 'overwrite'

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
