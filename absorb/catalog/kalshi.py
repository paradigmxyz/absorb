from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import datetime
    import polars as pl


url_template = 'https://kalshi-public-docs.s3.amazonaws.com/reporting/market_data_{year}-{month:02}-{day:02}.json'
path_template = '/Users/stormslivkoff/data/kalshi/raw_archive/market_data_{year}-{month:02}-{day:02}.json'


class Metrics(absorb.Table):
    source = 'kalshi'
    description = 'Daily summary data for each Kalshi market'
    url = 'https://kalshi.com/'
    write_range = 'append_only'
    chunk_size = 'day'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'ticker_name': pl.String,
            'old_ticker_name': pl.String,
            'report_ticker': pl.String,
            'payout_type': pl.String,
            'open_interest': pl.Float64,
            'daily_volume': pl.Int64,
            'block_volume': pl.Int64,
            'high': pl.Int64,
            'low': pl.Int64,
            'status': pl.String,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import requests
        import polars as pl

        date: datetime.datetime = chunk  # type: ignore
        url = get_date_url(date)
        response = requests.get(url, stream=True)
        if response.status_code == 404:
            return None
        response.raise_for_status()

        df = pl.DataFrame(response.json())
        if 'old_ticker_name' not in df.columns:
            df = df.insert_column(
                2, pl.lit(None, dtype=pl.String).alias('old_ticker_name')
            )
        df = df.rename({'date': 'timestamp'})

        return df

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        first = datetime.datetime(year=2021, month=6, day=30)
        last = _find_last()
        return (first, last)


class Metadata(absorb.Table):
    source = 'kalshi'
    description = 'Metadata for each Kalshi market'
    url = 'https://kalshi.com/'
    cadence = None
    write_range = 'overwrite_all'
    index_type = 'id'
    index_column = 'series_ticker'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'series_ticker': pl.String,
            'series_title': pl.String,
            'total_series_volume': pl.Int64,
            'total_volume': pl.Int64,
            'event_ticker': pl.String,
            'event_subtitle': pl.String,
            'event_title': pl.String,
            'category': pl.String,
            'total_market_count': pl.Int64,
            # 'product_metadata': pl.Struct,
            # 'markets': pl.List,
            'is_trending': pl.Boolean,
            'is_new': pl.Boolean,
            'is_closing': pl.Boolean,
            'is_price_delta': pl.Boolean,
            'search_score': pl.Int64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import requests
        import time
        import polars as pl

        base_url = 'https://api.elections.kalshi.com/v1/search/series?order_by=newest&page_size=100'

        cursor = None
        cursor_results: list[typing.Any] = []
        while True:
            if cursor is not None:
                url = base_url + '&cursor=' + cursor
            else:
                url = base_url
            time.sleep(0.25)
            print('getting kalshi metadata page', len(cursor_results) + 1)
            response = requests.get(url)
            data = response.json()
            if response.status_code == 200:
                cursor_results.append(data)
                cursor = data.get('next_cursor')
                if cursor is None:
                    break
                if len(cursor_results) * 100 > 2 * data['total_results_count']:
                    break
            else:
                print('status code', response.status_code)
                break

        return (
            pl.DataFrame(
                item
                for result in cursor_results
                for item in result['current_page']
            )
            .select(self.get_schema().keys())
            .unique('series_ticker')
        )

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        now = datetime.datetime.now(datetime.timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return (now, now)


def get_date_url(date: datetime.datetime) -> str:
    return url_template.format(year=date.year, month=date.month, day=date.day)


def get_date_path(date: datetime.datetime) -> str:
    return path_template.format(year=date.year, month=date.month, day=date.day)


def _find_last() -> datetime.datetime:
    import datetime

    current = datetime.datetime.now()
    current = datetime.datetime(
        year=current.year, month=current.month, day=current.day
    )
    while current > datetime.datetime(year=2021, month=6, day=28):
        if absorb.ops.does_remote_file_exist(get_date_url(current)):
            return current
        current = current - datetime.timedelta(days=1)
    raise Exception()
