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
        df = df.with_columns(
            pl.col.timestamp.str.to_datetime().dt.replace_time_zone('UTC')
        )

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
            'event_ticker': pl.String,
            'contract_ticker': pl.String,
            'category': pl.String,
            'series_title': pl.String,
            'event_title': pl.String,
            'event_subtitle': pl.String,
            'contract_title': pl.String,
            'open_timestamp': pl.Datetime(time_unit='us', time_zone='UTC'),
            'expected_expiration_timestamp': pl.Datetime(
                time_unit='us', time_zone='UTC'
            ),
            'close_timestamp': pl.Datetime(time_unit='us', time_zone='UTC'),
            'outcome': pl.String,
            'total_series_volume': pl.Int64,
            'total_event_volume': pl.Int64,
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
            .unique('event_ticker')
            .explode('markets')
            .with_columns(pl.col.markets.struct.unnest())
            .select(
                series_ticker=pl.col.series_ticker,
                event_ticker=pl.col.event_ticker,
                contract_ticker=pl.col.ticker,
                category=pl.col.category,
                series_title=pl.col.series_title,
                event_title=pl.col.event_title,
                event_subtitle=pl.col.event_subtitle,
                contract_title=pl.col.yes_subtitle,
                open_timestamp=pl.col.open_ts.str.to_datetime(),
                expected_expiration_timestamp=pl.col.expected_expiration_ts.str.to_datetime(),
                close_timestamp=pl.col.close_ts.str.to_datetime(),
                outcome=pl.col.result,
                total_series_volume=pl.col.total_series_volume,
                total_event_volume=pl.col.total_volume,
            )
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


def get_series_data(series_ticker):
    import requests

    url_template = 'https://api.elections.kalshi.com/v1/series/{series_ticker}'
    url = url_template.format(series_ticker=series_ticker)
    response = requests.get(url)
    return response.json()


def get_event_data(event_ticker, series_ticker=None):
    import requests

    if series_ticker is None:
        series_ticker = '-'.join(event_ticker.split('-')[:-1])

    url_template = 'https://api.elections.kalshi.com/v1/series/{series_ticker}/events/{event_ticker}'
    url = url_template.format(
        series_ticker=series_ticker, event_ticker=event_ticker
    )
    response = requests.get(url)
    return response.json()


def get_events_of_series(series_ticker, page=1):
    import requests

    url_template = 'https://api.elections.kalshi.com/v1/events/?series_tickers={series_ticker}&single_event_per_series=false&page_size=100&page_number={page}'
    url = url_template.format(series_ticker=series_ticker, page=page)
    response = requests.get(url)
    return response.json()
