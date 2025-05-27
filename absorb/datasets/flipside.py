from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl
    import flipside  # type: ignore


class Query(absorb.Table):
    source = 'flipside'
    write_range = 'overwrite_all'
    parameters = {'name': str, 'sql': str}

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        raise NotImplementedError('implement schema for Query')

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        sql = self.parameters['sql']
        return query(sql)


_clients = {
    'default': None,
}


def get_flipside_api_key() -> str:
    import os

    api_key = os.environ.get('FLIPSIDE_API_KEY')
    if api_key is None or api_key == '':
        raise Exception(
            'set FLIPSIDE_API_KEY in env var or use set_flipside_api_key() function'
        )
    else:
        return api_key


def set_flipside_api_key(api_key: str) -> None:
    import os

    os.environ['FLIPSIDE_API_KEY'] = api_key


def get_flipside_client(
    *, use_default: bool = True, api_key: str | None = None
) -> flipside.Flipside:
    import flipside

    if use_default and _clients['default'] is not None:
        return _clients['default']
    else:
        if api_key is None:
            api_key = get_flipside_api_key()
        client = flipside.Flipside(api_key, 'https://api-v2.flipsidecrypto.xyz')
        _clients['default'] = client
        return client


def query(sql: str) -> pl.DataFrame:
    import polars as pl

    client = get_flipside_client()
    result = client.query(sql)

    return pl.DataFrame(
        result.rows,
        schema=result.columns,
        orient='row',
        infer_schema_length=len(result.rows),
    )
