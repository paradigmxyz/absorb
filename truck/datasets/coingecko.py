from __future__ import annotations

import typing

import truck

if typing.TYPE_CHECKING:
    import polars as pl


endpoints = {
    'coin_list': 'https://api.coingecko.com/api/v3/coins/list',
    'current_coin_prices': 'https://api.coingecko.com/api/v3/coins/markets',
    'historical_coin_prices': 'https://api.coingecko.com/api/v3/coins/{id}/market_chart',
}


class CoinMetrics(truck.Table):
    source = 'coingecko'
    write_range = 'overwrite_all'
    range_format = 'date_range'
    parameter_types = {'top_n': int}

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'timestamp': pl.Datetime(time_unit='ms'),
            'coin': pl.String,
            'price': pl.Float64,
            'market_cap_usd': pl.Float64,
            'volume_usd': pl.Float64,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        return get_historical_coin_metrics(self.parameters['top_n'])

    def get_available_range(self) -> typing.Any:
        import datetime

        now = datetime.datetime.now()
        now = datetime.datetime(year=now.year, month=now.month, day=now.day)
        return (now - datetime.timedelta(days=364), now)


def _fetch(
    datatype: str,
    *,
    url_params: dict[str, typing.Any] | None = None,
    params: dict[str, typing.Any] | None = None,
    api_key: str | None = None,
) -> typing.Any:
    import requests

    headers = {'accept': 'application/json'}
    if api_key is None:
        api_key = get_coinbase_api_key()
    if api_key is not None:
        headers['x-cg-demo-api-key'] = api_key

    if url_params is None:
        url_params = {}
    url = endpoints[datatype].format(**url_params)
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def get_coinbase_api_key() -> str | None:
    import os

    return os.environ.get('COINGECKO_API_KEY')


#
# # dataframe fetching
#


def get_coin_list() -> pl.DataFrame:
    import polars as pl

    result = _fetch('coin_list')
    return pl.DataFrame(result)


def get_current_coin_prices(top_n: int = 1000) -> pl.DataFrame:
    import time
    import math
    import polars as pl

    n_pages = math.ceil(top_n / 250)
    results = []
    for page in range(1, n_pages + 1):
        params = {
            'vs_currency': 'usd',
            'per_page': 250,
            'price_change_percentage': '7d,14d,30d,200d,1y',
            'page': page,
        }
        result = _fetch('current_coin_prices', params=params)
        results.extend(result)
        time.sleep(2)
    return pl.DataFrame(results)[:top_n]


def get_historical_coin_metrics(
    coins: pl.Series | list[str] | int | None = None,
) -> pl.DataFrame:
    import time
    import polars as pl

    if coins is None:
        coins = get_current_coin_prices()['id']
        time.sleep(5)
    elif isinstance(coins, int):
        coins = get_current_coin_prices(coins)['id']
        time.sleep(5)

    print('getting historical data for', len(coins), 'coins')
    dfs = []
    for t, coin in enumerate(coins, start=1):
        print(str(t) + '. ' + coin)

        # get data
        params = {'vs_currency': 'usd', 'days': 365, 'interval': 'daily'}
        result = _fetch(
            'historical_coin_prices', url_params={'id': coin}, params=params
        )

        # parse into dataframes
        schema: dict[str, pl.DataType | type[pl.DataType]]
        schema = {'timestamp': pl.Datetime('ms'), 'price': pl.Float64}
        prices = pl.DataFrame(result['prices'], schema=schema, orient='row')
        schema = {'timestamp': pl.Datetime('ms'), 'market_cap_usd': pl.Float64}
        cap = pl.DataFrame(result['market_caps'], schema=schema, orient='row')
        schema = {'timestamp': pl.Datetime('ms'), 'volume_usd': pl.Float64}
        vol = pl.DataFrame(result['total_volumes'], schema=schema, orient='row')
        df = prices.join(cap, on='timestamp').join(vol, on='timestamp')
        df = df.insert_column(1, pl.lit(coin).alias('coin'))
        dfs.append(df)

        time.sleep(6)

    return pl.concat(dfs)

