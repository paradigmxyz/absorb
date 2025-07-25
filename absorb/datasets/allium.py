from __future__ import annotations

import typing

import absorb
from . import snowflake

if typing.TYPE_CHECKING:
    import polars as pl


class Query(snowflake.Query):
    source = 'allium'
    url = 'https://docs.allium.so/historical-data/overview'


class StablecoinSupply(Query):
    description = 'Stablecoin supply data from Allium'
    url = 'https://docs.allium.so/historical-data/stablecoins'

    sql = 'SELECT * FROM CROSSCHAIN_ALLIUM.STABLECOIN.SUPPLY_BETA'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'DATE': pl.Datetime(time_unit='ns', time_zone=None),
            'CHAIN': pl.String,
            'TOKEN_ADDRESS': pl.String,
            'TOKEN_SYMBOL': pl.String,
            'TOKEN_NAME': pl.String,
            'BASE_ASSET': pl.String,
            'SUPPLY': pl.Float64,
            'SUPPLY_USD': pl.Float64,
            'SUPPLY_DELTA': pl.Float64,
            'SUPPLY_DELTA_USD': pl.Float64,
            'IS_BRIDGE': pl.Boolean,
            'CURRENCY': pl.String,
            'UNIQUE_ID': pl.String,
            '_CREATED_AT': pl.Datetime(time_unit='ns', time_zone=None),
            '_UPDATED_AT': pl.Datetime(time_unit='ns', time_zone=None),
            '_CHANGED_SINCE_FULL_REFRESH': pl.Boolean,
        }
