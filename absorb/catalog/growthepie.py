# https://docs.growthepie.xyz/api

from __future__ import annotations

import absorb

import typing

if typing.TYPE_CHECKING:
    import polars as pl


class Metrics(absorb.Table):
    description = 'On-chain metrics for Ethereum and its rollups'
    url = 'https://www.growthepie.com/'
    source = 'growthepie'
    write_range = 'overwrite_all'
    row_precision = 'day'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'network': pl.String,
            'market_cap_usd': pl.Float64,
            'market_cap_eth': pl.Float64,
            'txcount': pl.Float64,
            'aa_last7d': pl.Float64,
            'txcosts_median_eth': pl.Float64,
            'daa': pl.Float64,
            'gas_per_second': pl.Float64,
            'fees_paid_eth': pl.Float64,
            'profit_usd': pl.Float64,
            'app_fees_eth': pl.Float64,
            'fees_paid_usd': pl.Float64,
            'txcosts_median_usd': pl.Float64,
            'stables_mcap_eth': pl.Float64,
            'fdv_eth': pl.Float64,
            'stables_mcap': pl.Float64,
            'costs_l1_usd': pl.Float64,
            'costs_total_usd': pl.Float64,
            'rent_paid_usd': pl.Float64,
            'app_fees_usd': pl.Float64,
            'tvl': pl.Float64,
            'tvl_eth': pl.Float64,
            'fdv_usd': pl.Float64,
            'costs_blobs_usd': pl.Float64,
            'rent_paid_eth': pl.Float64,
            'costs_blobs_eth': pl.Float64,
            'profit_eth': pl.Float64,
            'costs_l1_eth': pl.Float64,
            'costs_total_eth': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import requests
        import polars as pl

        url = 'https://api.growthepie.xyz/v1/fundamentals_full.json'
        response = requests.get(url)
        data = response.json()
        return (
            pl.DataFrame(data)
            .with_columns(
                timestamp=pl.col.date.str.to_date().cast(
                    pl.Datetime('us', 'UTC')
                )
            )
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
