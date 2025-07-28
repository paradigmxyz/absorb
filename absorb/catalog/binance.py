"""functions for fetching data from https://data.binance.vision/"""

from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import datetime
    import polars as pl


CandleInterval = typing.Literal[
    '1s',
    '1m',
    '3m',
    '5m',
    '15m',
    '30m',
    '1h',
    '2h',
    '4h',
    '6h',
    '8h',
    '12h',
    '1d',
]


class SpotCandles(absorb.Table):
    source = 'binance'
    description = 'OHLCV candles for spot pairs at various time intervals'
    url = 'https://data.binance.vision/?prefix=data/spot/daily/klines/'
    write_range = 'append_only'
    chunk_size = 'day'
    parameter_types = {'pair': str, 'interval': str}
    default_parameters = {}
    name_template = 'spot_candles_{pair}_{interval}'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'pair': pl.String,
            'open': pl.Float64,
            'high': pl.Float64,
            'low': pl.Float64,
            'close': pl.Float64,
            'n_trades': pl.Int64,
            'base_volume': pl.Float64,
            'quote_volume': pl.Float64,
            'taker_buy_base_volume': pl.Float64,
            'taker_buy_quote_volume': pl.Float64,
        }

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        return (
            datetime.datetime(2025, 1, 1),
            datetime.datetime(2025, 5, 1),
        )

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return get_spot_candles(
            pair=self.parameters['pair'],
            timestamp=chunk,  # type: ignore
            interval=self.parameters['interval'],
            window='daily',
        )


class SpotTrades(absorb.Table):
    source = 'binance'
    description = 'Trades for a given spot pair'
    url = 'https://data.binance.vision/?prefix=data/spot/daily/trades/'
    write_range = 'append_only'
    chunk_size = 'day'
    parameter_types = {'pair': str}
    default_parameters = {}
    name_template = 'spot_trades_{pair}'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'pair': pl.String,
            'price': pl.Float64,
            'quantity_base': pl.Float64,
            'quantity_quote': pl.Float64,
            'buyer_is_maker': pl.Boolean,
            'best_price_match': pl.Boolean,
            'trade_id': pl.Int64,
        }

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        return (
            datetime.datetime(2025, 1, 1),
            datetime.datetime(2025, 5, 1),
        )

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return get_spot_trades(
            pair=self.parameters['pair'],
            timestamp=chunk,  # type: ignore
            window='daily',
        )


class SpotAggregateTrades(absorb.Table):
    source = 'binance'
    description = (
        'Trades aggregated by price for spot pairs on short time scales'
    )
    url = 'https://data.binance.vision/?prefix=data/spot/daily/aggTrades/'
    write_range = 'append_only'
    chunk_size = 'day'
    parameter_types = {'pair': str}
    default_parameters = {}
    name_template = 'spot_aggregate_trades_{pair}'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'pair': pl.String,
            'price': pl.Float64,
            'quantity': pl.Float64,
            'buyer_is_maker': pl.Boolean,
            'best_price_match': pl.Boolean,
            'aggregate_trade_id': pl.Int64,
            'first_trade_id': pl.Int64,
            'last_trade_id': pl.Int64,
        }

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        return (
            datetime.datetime(2025, 1, 1),
            datetime.datetime(2025, 5, 1),
        )

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return get_spot_aggregate_trades(
            pair=self.parameters['pair'],
            timestamp=chunk,  # type: ignore
            window='daily',
        )


def get_spot_url(
    *,
    pair: str,
    timestamp: datetime.datetime,
    datatype: typing.Literal['trades', 'aggTrades', 'klines'],
    window: typing.Literal['daily', 'monthly'],
    interval: CandleInterval | None = None,
) -> str:
    import datetime
    import os

    if window == 'daily':
        if timestamp != datetime.datetime(
            timestamp.year, timestamp.month, timestamp.day
        ):
            raise Exception('timestamp must be a specific day')
        date_str = timestamp.strftime('%Y-%m-%d')
    elif window == 'monthly':
        if timestamp != datetime.datetime(timestamp.year, timestamp.month, 1):
            raise Exception('timestamp must be a specific month')
        date_str = timestamp.strftime('%Y-%m')
    else:
        raise Exception('invalid interval, choose daily or monthly')

    if datatype == 'klines':
        if interval is None:
            raise Exception('must specify interval')
        template = 'spot/{window}/klines/{pair}/{interval}/{pair}-{interval}-{date_str}.zip'
    else:
        if interval is not None:
            raise Exception(
                'cannot specify interval for dataset, only specify window'
            )
        template = (
            'spot/{window}/{datatype}/{pair}/{pair}-{datatype}-{date_str}.zip'
        )

    root = 'https://data.binance.vision/data/'
    tail = template.format(
        datatype=datatype,
        pair=pair,
        window=window,
        interval=interval,
        date_str=date_str,
    )
    return root + tail


def get_spot_trades(
    pair: str,
    timestamp: datetime.datetime,
    window: typing.Literal['daily', 'monthly'] = 'daily',
) -> pl.DataFrame:
    import polars as pl

    url = get_spot_url(
        pair=pair,
        timestamp=timestamp,
        datatype='trades',
        window=window,
    )

    raw_schema: dict[str, pl.DataType | type[pl.DataType]] = {
        'trade_id': pl.Int64,
        'price': pl.Float64,
        'quantity_base': pl.Float64,
        'quantity_quote': pl.Float64,
        'timestamp': pl.Int64,
        'buyer_is_maker': pl.Boolean,
        'best_price_match': pl.Boolean,
    }

    columns: list[str | pl.Expr] = [
        'timestamp',
        pl.lit(pair).alias('pair'),
        'price',
        'quantity_base',
        'quantity_quote',
        'buyer_is_maker',
        'best_price_match',
        'trade_id',
    ]

    return _process(url=url, raw_schema=raw_schema, columns=columns)


def get_spot_aggregate_trades(
    pair: str,
    timestamp: datetime.datetime,
    window: typing.Literal['daily', 'monthly'] = 'daily',
) -> pl.DataFrame:
    import polars as pl

    url = get_spot_url(
        pair=pair,
        timestamp=timestamp,
        datatype='aggTrades',
        window=window,
    )

    raw_schema: dict[str, pl.DataType | type[pl.DataType]] = {
        'aggregate_trade_id': pl.Int64,
        'price': pl.Float64,
        'quantity': pl.Float64,
        'first_trade_id': pl.Int64,
        'last_trade_id': pl.Int64,
        'timestamp': pl.Int64,
        'buyer_is_maker': pl.Boolean,
        'best_price_match': pl.Boolean,
    }

    columns: list[str | pl.Expr] = [
        'timestamp',
        pl.lit(pair).alias('pair'),
        'price',
        'quantity',
        'buyer_is_maker',
        'best_price_match',
        'aggregate_trade_id',
        'first_trade_id',
        'last_trade_id',
    ]

    return _process(url=url, raw_schema=raw_schema, columns=columns)


def get_spot_candles(
    pair: str,
    timestamp: datetime.datetime,
    interval: CandleInterval,
    window: typing.Literal['daily', 'monthly'] = 'daily',
) -> pl.DataFrame:
    import polars as pl

    url = get_spot_url(
        pair=pair,
        timestamp=timestamp,
        datatype='klines',
        interval=interval,
        window=window,
    )

    raw_schema: dict[str, pl.DataType | type[pl.DataType]] = {
        'timestamp': pl.Int64,
        'open': pl.Float64,
        'high': pl.Float64,
        'low': pl.Float64,
        'close': pl.Float64,
        'base_volume': pl.Float64,
        'close_timestamp': pl.Int64,
        'quote_volume': pl.Float64,
        'n_trades': pl.Int64,
        'taker_buy_base_volume': pl.Float64,
        'taker_buy_quote_volume': pl.Float64,
        'ignore': pl.String,
    }

    columns: list[str | pl.Expr] = [
        'timestamp',
        pl.lit(pair).alias('pair'),
        'open',
        'high',
        'low',
        'close',
        'n_trades',
        'base_volume',
        'quote_volume',
        'taker_buy_base_volume',
        'taker_buy_quote_volume',
    ]

    return _process(url=url, raw_schema=raw_schema, columns=columns)


def _process(
    url: str,
    raw_schema: dict[str, pl.DataType | type[pl.DataType]],
    columns: list[str | pl.Expr],
) -> pl.DataFrame:
    import polars as pl

    datetime_column = (
        pl.when(pl.col.timestamp >= 1230796800 * 1_000_000)
        .then(pl.col.timestamp.cast(pl.Datetime('us', 'UTC')))
        .when(pl.col.timestamp >= 1230796800 * 1_000)
        .then((1_000 * pl.col.timestamp).cast(pl.Datetime('us', 'UTC')))
        .when(pl.col.timestamp >= 1230796800)
        .then((1_000_000 * pl.col.timestamp).cast(pl.Datetime('us', 'UTC')))
    )

    return (
        absorb.ops.download_csv_zip_to_dataframe(
            url, polars_kwargs={'schema': raw_schema, 'has_header': False}
        )
        .with_columns(datetime_column)
        .select(columns)
    )
