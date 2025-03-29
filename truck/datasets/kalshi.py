from __future__ import annotations


url_template = 'https://kalshi-public-docs.s3.amazonaws.com/reporting/market_data_{year}-{month:02}-{day:02}.json'
path_template = '/Users/stormslivkoff/data/kalshi/raw_archive/market_data_{year}-{month:02}-{day:02}.json'


class Kalshi(Phylum.Dataset):
    datatypes = {
        'daily_summaries': KalshiDailySummaries,
        'metadata': KalshiMetadata,
    }


class KalshiDailySummaries(Phylum.DataType):
    overwrite = 'append_only'
    cadence = 'daily'

    def collect_date(cls, date: datetime.datetime) -> pl.DataFrame:
        url = get_date_url(date)
        response = requests.get(url, stream=True)
        response.raise_for_status()
        return pl.DataFrame(response.json())


class KalshiMetadata():
    cadence = None

    def get_dataset(cls):
        base_url = 'https://api.elections.kalshi.com/v1/search/series?order_by=trending&page_size=100'

        cursor = None
        cursor_results = []
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

        print('collected', len(cursor_results), 'pages')


def get_date_url(date):
    return url_template.format(year=date.year, month=date.month, day=date.day)


def get_date_path(date):
    return path_template.format(year=date.year, month=date.month, day=date.day)

