from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


class Metric(absorb.Table):
    source = 'fred'
    write_range = 'overwrite_all'
    parameter_types = {'series_id': str}

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'date': pl.Datetime(),
            'raw_value': pl.Float64,
            'value': pl.Float64,
            'metric': pl.String,
        }

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        import polars as pl

        series_id = self.parameters['series_id']
        return get_series(series_id, rename=False).select(
            timestamp=pl.col.date.cast(pl.Datetime('ms')),
            raw_value='raw_value',
            value='value',
            metric=pl.lit(series_id),
        )

    def get_available_range(self) -> absorb.Coverage:
        import datetime

        metadata = get_series_metadata(self.parameters['series_id'])
        return (
            datetime.datetime.strptime(
                metadata['observation_start'], '%Y-%m-%d'
            ),
            datetime.datetime.strptime(metadata['observation_end'], '%Y-%m-%d'),
        )


def get_tables() -> list[type[absorb.Table]]:
    return tables


def get_api_key() -> str:
    import os

    api_key = os.environ['FRED_API_KEY']
    if api_key is None or api_key == '':
        raise Exception('must set FRED_API_KEY')
    return api_key


def get_series_catalog() -> dict[str, str]:
    return {
        # monetary
        'M1': 'M1SL',
        'M1_raw': 'M1NS',
        'M2': 'M2SL',
        'M2_raw': 'M2NS',
        # inflation
        'CPI': 'CPIAUCSL',
        'PCEPI': 'PCEPI',
        'PPI': 'PPIACO',
        # interest rates
        'interest_rates': 'FEDFUNDS',
        'bond_yield_10y': 'DGS10',
        # labor
        'unemployment': 'UNRATE',
        'nonfarm_employment': 'PAYEMS',
        'labor_force_participation': 'CIVPART',
        # economic output
        'nominal_gdp': 'GDP',
        'adjusted_gdp': 'GDPC1',
    }


def get_normalized_columns() -> dict[str, tuple[pl.Expr, str]]:
    import polars as pl

    return {
        'Billions of Chained 2017 Dollars': (
            pl.col.raw_value * 1e9,
            '_2017_usd',
        ),
        'Billions of Dollars': (pl.col.raw_value * 1e9, '_usd'),
        'Index 1982-1984=100': (pl.col.raw_value, '_index_1984'),
        'Index 1982=100': (pl.col.raw_value, '_index_1982'),
        'Index 2017=100': (pl.col.raw_value, '_index_2017'),
        'Percent': (pl.col.raw_value, '_percent'),
        'Thousands of Persons': ((pl.col.raw_value * 1000), '_people'),
    }


def _fetch(url: str, params: dict[str, str]) -> dict[str, typing.Any]:
    import requests

    params['api_key'] = get_api_key()
    params['file_type'] = 'json'
    response = requests.get(url, params=params)
    result: dict[str, typing.Any] = response.json()
    return result


def get_series_metadata(name: str) -> dict[str, typing.Any]:
    url = 'https://api.stlouisfed.org/fred/series'
    series_id = get_series_catalog().get(name, name)
    result = _fetch(url, {'series_id': series_id})
    if len(result['seriess']) == 0:
        raise Exception('series not found')
    elif len(result['seriess']) > 1:
        raise Exception('multiple series found')
    else:
        return result['seriess'][0]  # type: ignore


def get_series(
    name: str,
    *,
    parameters: dict[str, typing.Any] | None = None,
    normalize: bool = True,
    rename: bool = True,
) -> pl.DataFrame:
    import polars as pl

    url = 'https://api.stlouisfed.org/fred/series/observations'

    series_id = get_series_catalog().get(name, name)
    params = {'series_id': series_id}
    if parameters is not None:
        params.update(parameters)
    result = _fetch(url, params)
    df = pl.DataFrame(result['observations']).select(
        date=pl.col.date.str.to_date(),
        raw_value=pl.col.value.replace({'.': None}).cast(pl.Float64),
    )

    if normalize:
        normalized_columns = get_normalized_columns()
        metadata = get_series_metadata(name)
        if metadata['units'] in normalized_columns:
            column, suffix = normalized_columns[metadata['units']]
            if rename:
                new_name = name + suffix
            else:
                new_name = 'value'
            df = df.with_columns(column.alias(new_name))

    return df


def get_all_tags() -> pl.DataFrame:
    url = 'https://api.stlouisfed.org/fred/tags'

    offset = 0
    tags = []
    print('collecting all tags')
    while True:
        print('fetching', offset)
        result = _fetch(url, params={'offset': str(offset), 'limit': str(1000)})
        tags.extend(result['tags'])
        if len(result['tags']) == 0 or len(tags) >= result['count']:
            break
        else:
            offset += 1000

    return pl.DataFrame(tags)


# create classes
tables = []
for name in get_series_catalog().keys():
    namespace = {'default_parameters': {'series_id': name}}
    cls = type(name, (Metric,), namespace)
    tables.append(cls)
    globals()[name] = cls
