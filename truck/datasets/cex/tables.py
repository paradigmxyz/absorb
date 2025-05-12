from __future__ import annotations

import typing

import truck

if typing.TYPE_CHECKING:
    import polars as pl


class Candles(truck.Table):
    source = 'cex'
    write_range = 'append_only'
    parameters = {'cex': str, 'pair': str, 'interval': str, 'market': str}
    default_parameters = {'market': 'spot'}

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us'),
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


class Trades(truck.Table):
    source = 'cex'
    write_range = 'append_only'
    parameters = {'cex': str, 'pair': str, 'market': str}
    default_parameters = {'market': 'spot'}

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us'),
            'price': pl.Float64,
            'quantity_base': pl.Float64,
            'quantity_quote': pl.Float64,
            'buyer_is_maker': pl.Boolean,
            'best_price_match': pl.Boolean,
            'trade_id': pl.Int64,
        }


class AggregatedTrades(truck.Table):
    source = 'cex'
    write_range = 'append_only'
    parameters = {'cex': str, 'pair': str, 'market': str}
    default_parameters = {'market': 'spot'}

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us'),
            'price': pl.Float64,
            'quantity': pl.Float64,
            'buyer_is_maker': pl.Boolean,
            'best_price_match': pl.Boolean,
            'aggregate_trade_id': pl.Int64,
            'first_trade_id': pl.Int64,
            'last_trade_id': pl.Int64,
        }
