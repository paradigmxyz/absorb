from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import tooltime

from . import paths


class DataRange(typing.TypedDict):
    """specify data range using one of:
    1. time bounds
    2. non-temporal bounds
    3. list of chunks
    """
    # type: typing.
    start_time: datetime.datetime | None
    end_time: datetime.datetime | None
    start_bound: typing.Any | None
    end_bound: typing.Any | None
    chunks: list[typing.Any] | None


class DatasetDownload:
    cadence: typing.Literal['daily', 'weekly', 'monthly', 'yearly'] | None
    write_range: typing.Literal['append_only', 'overwrite']

    @classmethod
    def get_path(cls, date: datetime.datetime, context: PhylumContext) -> str:
        path_template = '/Users/stormslivkoff/data/kalshi/raw_archive/market_data_{year}-{month:02}-{day:02}.json'
        return path_template.format(
            year=date.year, month=date.month, day=date.day
        )

    @classmethod
    def update_dataset(
        cls,
        context: PhylumContext | None = None,
        data_range: DataRange | None = None,
        overwrite: bool = False,
    ) -> None:
        if data_range is None:
            collected = cls.get_range_collected()
            available = cls.get_range_available()
            missing = None

        if cls.cadence == 'daily':
            dates = tooltime.get_intervals(
                start='2021-06-28',
                end=,
                interval='1d',
            )['start']
            df = cls.collect_date(date)
            path = cls.get_path(date=date)
            df.write_parquet(df)
        else:
            raise Exception()

    @classmethod
    def get_range_available(cls) -> DataRange:
        pass

    @classmethod
    def get_range_collected() -> DataRange:
        pass

    @classmethod
    def get_local_path() -> str:
        pass


def download(
    start_time: tooltime.Timestamp,
    end_time: tooltime.Timestamp,
    root_dir: str | None = None,
) -> None:
    import os
    import tooltime

    # determine timestamps
    timestamps = tooltime.get_intervals(
        start=start_time, end=end_time, interval='1d'
    )['start']

    # summmarize
    if root_dir is None:
        root_dir = paths.get_root_dir()
    print('downloading', len(timestamps), 'files to:', root_dir)

    # download files
    for timestamp in timestamps:
        url = get_url(timestamp=timestamp)
        output_path = paths.get_path(timestamp=timestamp, root_dir=root_dir)
        if os.path.exists(output_path):
            print('already downloaded', timestamp)
        else:
            print('downloading', timestamp)
            _download_file(url=url, output_path=output_path)


def get_url(timestamp: tooltime.Timestamp) -> str:
    return paths.get_path(
        timestamp=timestamp,
        root_dir='https://mempool-dumpster.flashbots.net/ethereum/mainnet',
        flat=False,
    )


def _download_file(url: str, output_path: str) -> None:
    """generic downloader function"""
    import os
    import requests
    import shutil

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    tmp_path = output_path + '_tmp'
    with open(tmp_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
    shutil.move(tmp_path, output_path)

