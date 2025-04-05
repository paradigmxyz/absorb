# https://docs.growthepie.xyz/api

from __future__ import annotations

import truck

import typing

if typing.TYPE_CHECKING:
    import polars as pl


class Fundamentals(truck.Table):
    write_range = 'overwrite'

    @classmethod
    def get_schema(cls, context: truck.Context) -> dict[str, pl.Datatype]:
        return dict(cls.collect(context).schema)

    @classmethod
    def collect(cls, context: truck.Context) -> pl.DataFrame:
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
