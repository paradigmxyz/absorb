from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl


def does_remote_file_exist(url: str) -> bool:
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


def download_parquet_to_dataframe(url: str) -> pl.DataFrame:
    import io
    import requests
    import polars as pl

    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(
                f'Failed to download: HTTP status code {response.status_code}'
            )
        parquet_buffer = io.BytesIO(response.content)
        return pl.read_parquet(parquet_buffer)
    except Exception as e:
        raise Exception(f'Error processing parquet file: {str(e)}')


def download_csv_gz_to_dataframe(
    url: str, *, polars_kwargs: dict[str, typing.Any] | None = None
) -> pl.DataFrame:
    import io
    import gzip
    import requests
    import polars as pl

    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(
                f'Failed to download: HTTP status code {response.status_code}'
            )
        csv_buffer = io.StringIO(
            gzip.decompress(response.content).decode('utf-8')
        )
        if polars_kwargs is None:
            polars_kwargs = {}
        return pl.read_csv(csv_buffer, **polars_kwargs)
    except Exception as e:
        raise Exception(f'Error processing csv.gz file: {str(e)}')


def download_csv_zip_to_dataframe(
    url: str, *, polars_kwargs: dict[str, typing.Any] | None = None
) -> pl.DataFrame:
    import io
    import zipfile
    import requests
    import polars as pl

    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(
                f'Failed to download: HTTP status code {response.status_code}'
            )
        zip_buffer = io.BytesIO(response.content)
        with zipfile.ZipFile(zip_buffer, 'r') as z:
            csv_filename = [f for f in z.namelist() if f.endswith('.csv')][0]
            with z.open(csv_filename) as csv_file:
                csv_buffer = io.StringIO(csv_file.read().decode('utf-8'))
                if polars_kwargs is None:
                    polars_kwargs = {}
                return pl.read_csv(csv_buffer, **polars_kwargs)
    except Exception as e:
        raise Exception(f'Error processing csv.zip file: {str(e)}')
