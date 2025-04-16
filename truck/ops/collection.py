from __future__ import annotations

import typing

import truck
from . import paths

if typing.TYPE_CHECKING:
    import datetime
    import tooltime
    import polars as pl


def does_file_exist(url: str) -> bool:
    import requests

    try:
        response = requests.head(url, allow_redirects=True)
        # Check if status code is 200 (OK) and content-length exists
        if response.status_code == 200 and 'content-length' in response.headers:
            return True
        return False
    except requests.RequestException:
        return False


def download_file(*, url: str, path: str) -> None:
    raise NotImplementedError()


def write_file(*, df: pl.DataFrame, path: str) -> None:
    import shutil

    tmp_path = path + '_tmp'
    if path.endswith('.parquet'):
        df.write_parquet(tmp_path)
    elif path.endswith('.csv'):
        df.write_csv(tmp_path)
    else:
        raise Exception('invalid file extension')
    shutil.move(tmp_path, path)


# class DataRange(typing.TypedDict):
#     """specify data range using one of:
#     1. time bounds
#     2. non-temporal bounds
#     3. list of chunks
#     """

#     start_time: datetime.datetime | None
#     end_time: datetime.datetime | None
#     start_bound: typing.Any | None
#     end_bound: typing.Any | None
#     chunks: list[typing.Any] | None


# def get_path(table: truck.TableReference, date: datetime.datetime, context: truck.Context) -> str:
#     path_template = '/Users/stormslivkoff/data/kalshi/raw_archive/market_data_{year}-{month:02}-{day:02}.json'
#     return path_template.format(
#         year=date.year, month=date.month, day=date.day
#     )


# def collect(
#     table: truck.TableReference,
#     data_range: typing.Any,
#     overwrite: bool = False,
# ) -> None:
#     import tooltime

#     cls = truck.resolve_table_class(table)

#     if data_range is None:
#         collected = cls.get_collected_range()
#         available = cls.get_available_range()
#         missing = None

#     if cls.cadence == 'daily':
#         dates = tooltime.get_intervals(
#             start='2021-06-28',
#             end=tooltime.now(),
#             interval='1d',
#         )['start']
#         for date in dates:
#             date_context = context.copy()
#             date_context['data_range'] = date
#             df = cls.collect(data_range=data_range)
#             path = cls.get_path(date_context)
#             df.write_parquet(path)
#     else:
#         raise Exception()


# def download(
#     start_time: tooltime.Timestamp,
#     end_time: tooltime.Timestamp,
#     root_dir: str | None = None,
# ) -> None:
#     import os
#     import tooltime

#     # determine timestamps
#     timestamps = tooltime.get_intervals(
#         start=start_time, end=end_time, interval='1d'
#     )['start']

#     # summmarize
#     if root_dir is None:
#         root_dir = paths.get_truck_root()
#     print('downloading', len(timestamps), 'files to:', root_dir)

#     # download files
#     for timestamp in timestamps:
#         url = get_url(timestamp=timestamp)
#         output_path = paths.get_path(timestamp=timestamp, root_dir=root_dir)
#         if os.path.exists(output_path):
#             print('already downloaded', timestamp)
#         else:
#             print('downloading', timestamp)
#             _download_file(url=url, output_path=output_path)


# def get_url(timestamp: tooltime.Timestamp) -> str:
#     return paths.get_path(
#         timestamp=timestamp,
#         root_dir='https://mempool-dumpster.flashbots.net/ethereum/mainnet',
#         flat=False,
#     )


# def _download_file(url: str, output_path: str) -> None:
#     """generic downloader function"""
#     import os
#     import requests
#     import shutil

#     os.makedirs(os.path.dirname(output_path), exist_ok=True)
#     response = requests.get(url, stream=True)
#     response.raise_for_status()
#     tmp_path = output_path + '_tmp'
#     with open(tmp_path, 'wb') as file:
#         for chunk in response.iter_content(chunk_size=8192):
#             if chunk:
#                 file.write(chunk)
#     shutil.move(tmp_path, output_path)
