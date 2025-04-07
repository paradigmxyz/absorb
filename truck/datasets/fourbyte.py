from __future__ import annotations

import typing

import truck

if typing.TYPE_CHECKING:
    import polars as pl


class FourbyteDatatype(truck.Table):
    source = 'fourbyte'
    write_range = 'append_only'
    range_format = 'id_range'

    # custom
    endpoint: str

    def get_schema(self) -> dict[str, type[pl.DataType]]:
        import polars as pl

        return {
            'id': pl.Int64,
            'created_at': pl.Datetime,
            'text_signature': pl.String,
            'hex_signature': pl.String,
            'bytes_signature': pl.Binary,
        }

    def get_available_range(self) -> typing.Any:
        import requests

        results = requests.get(self.endpoint).json()
        max_id = max(result['id'] for result in results)
        return (0, max_id)

    async def async_collect(self, data_range: typing.Any) -> pl.DataFrame:
        return await async_scrape_4byte(
            url=self.endpoint, data_range=data_range
        )


class Functions(FourbyteDatatype):
    endpoint = 'https://www.4byte.directory/api/v1/signatures/'


class Events(FourbyteDatatype):
    endpoint = 'https://www.4byte.directory/api/v1/event-signatures/'


def get_tables() -> list[type[truck.Table]]:
    return [Functions, Events]


async def async_scrape_4byte(
    url: str,
    data_range: tuple[int, int],
    wait_time: float = 0.1,
    min_id: int | None = None,
) -> pl.DataFrame:
    import aiohttp
    import polars as pl

    results = []
    async with aiohttp.ClientSession() as session:
        while True:
            # get page
            async with session.get(url) as response_object:
                response = await response_object.json()
                results.extend(response['results'])

            # scrape only until min_id is reached
            if min_id is not None:
                min_result_id = min(
                    result['id'] for result in response['results']
                )
                if min_result_id < min_id:
                    break

            # get next url
            url = response['next']
            if url is None:
                break

            # wait between responses
            if wait_time is not None:
                import asyncio

                await asyncio.sleep(wait_time)

    return pl.DataFrame(results, orient='row')
