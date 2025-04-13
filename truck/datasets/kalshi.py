from __future__ import annotations

import typing

import truck

if typing.TYPE_CHECKING:
    import datetime
    import polars as pl


url_template = 'https://kalshi-public-docs.s3.amazonaws.com/reporting/market_data_{year}-{month:02}-{day:02}.json'
path_template = '/Users/stormslivkoff/data/kalshi/raw_archive/market_data_{year}-{month:02}-{day:02}.json'


class DailyStats(truck.Table):
    source = 'kalshi'
    overwrite = 'append_only'
    cadence = 'daily'
    range_format = 'date'

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        import requests
        import polars as pl

        date: datetime.datetime = data_range
        url = get_date_url(date)
        response = requests.get(url, stream=True)
        response.raise_for_status()
        return pl.DataFrame(response.json())

    def get_available_range(self) -> typing.Any:
        import datetime

        first = datetime.datetime(year=2021, month=6, day=28)
        last = _find_last()
        return [first, last]


class Metadata(truck.Table):
    source = 'kalshi'
    cadence = None

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        import requests
        import time

        base_url = 'https://api.elections.kalshi.com/v1/search/series?order_by=trending&page_size=100'

        cursor = None
        cursor_results: list[typing.Any] = []
        while True:
            if cursor is not None:
                url = base_url + '&cursor=' + cursor
            else:
                url = base_url
            time.sleep(0.25)
            print('getting page', len(cursor_results))
            response = requests.get(url)
            data = response.json()
            if response.status_code == 200:
                cursor_results.append(data)
                cursor = data.get('next_cursor')
                if cursor is None:
                    break
            else:
                print('status code', response.status_code)
                break

        return pl.DataFrame(
            item for result in cursor_results for item in result['current_page']
        ).unique('series_ticker')

    def get_available_range(self) -> typing.Any:
        import datetime

        now = datetime.datetime.now(datetime.timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return [None, now]


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
        if truck.ops.collection.does_file_exist(get_date_url(current)):
            return current
        current = current - datetime.timedelta(days=1)
    raise Exception()
