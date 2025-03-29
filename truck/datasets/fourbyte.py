from __future__ import annotations

import typing

import truck


class Fourbyte(Truck.Dataset):
    pass


class FourbyteDatatype(Truck.Datatype):
    dataset = Fourbyte
    range_format = (int, int)
    endpoint: str

    @classmethod
    def get_schema(cls):
        import polars as pl

        return {
            'id': pl.Int64,
            'created_at': pl.Datetime,
            'text_signature': pl.String,
            'hex_signature': pl.String,
            'bytes_signature': pl.Binary,
        }

    @classmethod
    def get_available_range(cls) -> typing.Any:
        return get_available_range(cls.endpoint)

    @classmethod
    async def async_collect(cls, data_range, context) -> pl.DataFrame:
        return await async_scrape_4byte(url=cls.endpoint, data_range=data_range)


class FunctionSignatures(FourbyteDatatype):
    endpoint = 'https://www.4byte.directory/api/v1/signatures/'


class EventSignatures(FourbyteDatatype):
    endpoint = 'https://www.4byte.directory/api/v1/event-signatures/'


def get_available_range(url: str) -> (int, int):
    import requests

    results = requests.get(cls.endpoint).json()
    max_id = max(result['id'] for result in results)
    return (0, max_id)


async def async_scrape_4byte(
    url: str, data_range: tuple[int, int], wait_time: float = 0.1
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
