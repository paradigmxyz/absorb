from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


endpoints = {
    'coin_list': 'https://api.coingecko.com/api/v3/coins/list',
    'category_list': 'https://api.coingecko.com/api/v3/coins/categories/list',
    'current_coin_prices': 'https://api.coingecko.com/api/v3/coins/markets',
    'historical_coin_prices': 'https://api.coingecko.com/api/v3/coins/{id}/market_chart',
}


class CoinMetrics(absorb.Table):
    source = 'coingecko'
    description = 'Price, market cap, and volume data for coins'
    url = 'https://coingecko.com/'
    write_range = 'overwrite_all'
    index_type = 'temporal'
    parameter_types = {'top_n': int}
    default_parameters = {'top_n': None}
    name_template = [
        'coin_metrics_top_{top_n}',
        'coin_metrics',
    ]

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'coin': pl.String,
            'price': pl.Float64,
            'market_cap_usd': pl.Float64,
            'volume_usd': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        top_n = self.parameters['top_n']
        if top_n is None:
            top_n = 1000
        return get_historical_coin_metrics(top_n)

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        now = datetime.datetime.now()
        now = datetime.datetime(year=now.year, month=now.month, day=now.day)
        return (now - datetime.timedelta(days=364), now)


class Categories(absorb.Table):
    source = 'coingecko'
    description = 'Categorizations of coins'
    url = 'https://coingecko.com/'
    write_range = 'overwrite_all'
    index_type = 'id'
    index_column = ('coin', 'category')
    parameter_types = {'categories': (list, type(None))}
    default_parameters = {'categories': None}
    name_template = [
        'coin_metrics_{categories}',
        'coin_metrics',
    ]

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {'coin': pl.String, 'category': pl.String}

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return get_current_coin_categories(
            categories=self.parameters['categories']
        )

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        now = datetime.datetime.now()
        now = datetime.datetime(year=now.year, month=now.month, day=now.day)
        return (now, now)


class CategoryMetrics(absorb.Table):
    source = 'coingecko'
    description = 'Aggregated metrics for each category of coins'
    url = 'https://coingecko.com/'
    write_range = 'overwrite_all'
    index_type = 'temporal'
    parameter_types = {'categories': (list, type(None))}
    default_parameters = {'categories': None}
    dependencies = [CoinMetrics, Categories]
    name_template = [
        'category_metrics_{categories}',
        'category_metrics',
    ]

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'category': pl.String,
            'market_cap_usd': pl.Float64,
            'volume_usd': pl.Float64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        coin_metrics = absorb.ops.load('coingecko.coin_metrics')
        coin_categories = absorb.ops.load('coingecko.categories')
        return get_historical_category_metrics(
            coin_metrics=coin_metrics, coin_categories=coin_categories
        )

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        now = datetime.datetime.now()
        now = datetime.datetime(year=now.year, month=now.month, day=now.day)
        return (now, now)


def _fetch(
    datatype: str,
    *,
    url_params: dict[str, typing.Any] | None = None,
    params: dict[str, typing.Any] | None = None,
    api_key: str | None = None,
) -> typing.Any:
    import time
    import requests

    headers = {'accept': 'application/json'}
    if api_key is None:
        api_key = get_coinbase_api_key()
    if api_key is not None:
        headers['x-cg-demo-api-key'] = api_key

    if url_params is None:
        url_params = {}
    url = endpoints[datatype].format(**url_params)
    # response = session.get(url, headers=headers, params=params)

    n_attempts = 5
    for i in range(n_attempts):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:
            time.sleep(60 * (i + 1))
            continue
        response.raise_for_status()
        return response.json()
    raise Exception(
        'Failed to fetch data from CoinGecko API after '
        + str(n_attempts)
        + ' attempts.'
    )


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


def get_category_list(top_n: int | None = None) -> pl.DataFrame:
    import polars as pl

    return pl.DataFrame(_fetch('category_list'))


def get_current_coin_prices(
    top_n: int = 1000,
    *,
    category: str | None = None,
    include_price_changes: bool = False,
) -> pl.DataFrame:
    import time
    import math
    import polars as pl

    n_pages = math.ceil(top_n / 250)
    results = []
    for page in range(1, n_pages + 1):
        params = {'vs_currency': 'usd', 'per_page': 250, 'page': page}
        if category is not None:
            params['category'] = category
        if include_price_changes:
            params['price_change_percentage'] = '7d,14d,30d,200d,1y'
        result = _fetch('current_coin_prices', params=params)
        results.extend(result)
        time.sleep(6)
        if len(result) == 0:
            break
    schema = {
        'id': pl.String,
        'symbol': pl.String,
        'name': pl.String,
        'image': pl.String,
        'current_price': pl.Float64,
        'market_cap': pl.Float64,
        'market_cap_rank': pl.Int64,
        'fully_diluted_valuation': pl.Float64,
        'total_volume': pl.Float64,
        'high_24h': pl.Float64,
        'low_24h': pl.Float64,
        'price_change_24h': pl.Float64,
        'price_change_percentage_24h': pl.Float64,
        'market_cap_change_24h': pl.Float64,
        'market_cap_change_percentage_24h': pl.Float64,
        'circulating_supply': pl.Float64,
        'total_supply': pl.Float64,
        'max_supply': pl.Float64,
        'ath': pl.Float64,
        'ath_change_percentage': pl.Float64,
        'ath_date': pl.String,
        'atl': pl.Float64,
        'atl_change_percentage': pl.Float64,
        'atl_date': pl.String,
        # 'roi': pl.Struct({'times': Float64, 'currency': String, 'percentage': Float64}),
        'last_updated': pl.String,
    }
    return pl.DataFrame(results, schema=schema, infer_schema_length=top_n)[
        :top_n
    ]


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
        print('[' + str(t) + ' / ' + str(len(coins)) + '] ' + coin)

        # get data
        params = {'vs_currency': 'usd', 'days': 365, 'interval': 'daily'}
        result = _fetch(
            'historical_coin_prices', url_params={'id': coin}, params=params
        )

        # parse into dataframes
        dt = pl.Datetime('us', 'UTC')
        schema: dict[str, pl.DataType | type[pl.DataType]]
        schema = {'timestamp': dt, 'price': pl.Float64}
        prices = pl.DataFrame(result['prices'], schema=schema, orient='row')
        schema = {'timestamp': dt, 'market_cap_usd': pl.Float64}
        cap = pl.DataFrame(result['market_caps'], schema=schema, orient='row')
        schema = {'timestamp': dt, 'volume_usd': pl.Float64}
        vol = pl.DataFrame(result['total_volumes'], schema=schema, orient='row')
        df = prices.join(cap, on='timestamp').join(vol, on='timestamp')
        df = df.insert_column(1, pl.lit(coin).alias('coin'))
        dfs.append(df)

        time.sleep(6)

    return pl.concat(dfs)


def get_current_coin_categories(
    categories: list[str] | pl.Series | None = None,
) -> pl.DataFrame:
    import time
    import polars as pl

    if categories is None:
        categories = get_category_list()['category_id']
        time.sleep(10)

    print('getting tokens for', len(categories), 'categories')
    dfs = []
    for c, category in enumerate(categories, start=1):
        print(str(c) + '. ' + category)
        result = get_current_coin_prices(category=category)
        if len(result) > 0:
            df = result.select(coin='id', category=pl.lit(category))
            dfs.append(df)
        time.sleep(30)
    return pl.concat(dfs)


def get_historical_category_metrics(
    *,
    coin_metrics: pl.DataFrame | None = None,
    coin_categories: pl.DataFrame | None = None,
) -> pl.DataFrame:
    import polars as pl

    if coin_metrics is None:
        coin_metrics = get_historical_coin_metrics()
    if coin_categories is None:
        coin_categories = get_current_coin_categories()
    return (
        coin_metrics.join(coin_categories, on='coin', how='left')
        .group_by('timestamp', 'category')
        .agg(pl.sum('market_cap_usd'), pl.sum('volume_usd'))
        .sort('timestamp', 'category')
    )


def _convert_current_prices_to_changes(prices: pl.DataFrame) -> pl.DataFrame:
    import datetime

    intervals = ['7d', '14d', '30d', '200d', '1y']

    volume_columns = {
        ('value_' + interval): pl.col.current_price
        / (
            1
            + pl.col('price_change_percentage_' + interval + '_in_currency')
            / 100
        )
        for interval in intervals
    }
    abs_delta_columns = {
        'abs_delta_' + interval: pl.col.value_now - pl.col('value_' + interval)
        for interval in intervals
    }
    rel_delta_columns = {
        'rel_delta_' + interval: (
            pl.col.value_now - pl.col('value_' + interval)
        )
        / pl.col('value_' + interval)
        for interval in intervals
    }
    t_columns = {
        't_' + interval: pl.col.t_now.dt.offset_by('-' + interval)
        for interval in intervals
    }

    return (
        prices.select(
            token='id',
            value_now='current_price',
            **volume_columns,
        )
        .with_columns(
            **abs_delta_columns,
            **rel_delta_columns,
            t_now=pl.lit(datetime.datetime.now()),
        )
        .with_columns(
            **t_columns,
        )
    )
