from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


class FourbyteDatatype(absorb.Table):
    source = 'fourbyte'
    url = 'https://www.4byte.directory/'
    write_range = 'append_only'
    chunk_size = 10000
    index_column = 'id'

    # custom
    endpoint: str

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'id': pl.Int64,
            'created_at': pl.Datetime('us', 'UTC'),
            'text_signature': pl.String,
            'hex_signature': pl.String,
            'bytes_signature': pl.Binary,
        }

    def get_available_range(self) -> absorb.Coverage:
        import requests

        data = requests.get(self.endpoint).json()
        max_id = max(result['id'] for result in data['results'])
        return (0, max_id)

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return scrape_4byte(url=self.endpoint, chunk=chunk)  # type: ignore


class Functions(FourbyteDatatype):
    description = 'Functions listed on 4byte.directory'
    endpoint = 'https://www.4byte.directory/api/v1/signatures/'


class Events(FourbyteDatatype):
    description = 'Event types listed on 4byte.directory'
    endpoint = 'https://www.4byte.directory/api/v1/event-signatures/'


def get_tables() -> list[type[absorb.Table]]:
    return [Functions, Events]


def scrape_4byte(
    url: str,
    chunk: tuple[int, int],
    wait_time: float = 0.1,
    min_id: int | None = None,
) -> pl.DataFrame:
    import requests
    import polars as pl
    import time

    results = []
    while True:
        # get page
        response = requests.get(url)
        result: dict[str, typing.Any] = response.json()
        results.extend(result['results'])

        # scrape only until min_id is reached
        if min_id is not None:
            min_result_id = min(result['id'] for result in result['results'])
            if min_result_id < min_id:
                break

        # get next url
        url = result['next']
        if url is None:
            break

        # wait between responses
        if wait_time is not None:
            time.sleep(wait_time)

    return pl.DataFrame(results, orient='row')
