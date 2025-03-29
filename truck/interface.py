from __future__ import annotations

import typing

from . import dataset_class

if typing.TYPE_CHECKING:
    import datetime


def _resolve_dataset(dataset: DatasetReference) -> type[dataset_class.Dataset]:
    import importlib

    module = importlib.import('datum.datasets.')


def scan(
    dataset: DatasetReference,
    *,
    start_time: tooltime.Timestamp | None = None,
    end_time: tooltime.Timestamp | None = None,
) -> pl.LazyFrame:
    dataset_class = _resolve_dataset_class(dataset)
    dataset_class.scan(
        start_time=start_time,
        end_time=end_time,
    )


def load(
    dataset: DatasetReference,
    *,
    start_time: tooltime.Timestamp | None = None,
    end_time: tooltime.Timestamp | None = None,
) -> pl.DataFrame:
    dataset_class = _resolve_dataset_class(dataset)
    dataset_class.load(
        start_time=start_time,
        end_time=end_time,
    )


def download(
    dataset: DatasetReference,
    *,
    start_time: tooltime.Timestamp | None = None,
    end_time: tooltime.Timestamp | None = None,
) -> None:
    dataset_class = _resolve_dataset_class(dataset)
    dataset_class.download(
        start_time=start_time,
        end_time=end_time,
    )



def get_min_local_timestamp(dataset: DatasetReference) -> datetime.datetime:
    return _resolve_dataset_class(dataset).get_min_local_timestamp()


def get_max_local_timestamp(dataset: DatasetReference) -> datetime.datetime:
    return _resolve_dataset_class(dataset).get_max_local_timestamp()


def get_min_available_timestamp(dataset: DatasetReference) -> datetime.datetime:
    return _resolve_dataset_class(dataset).get_min_available_timestamp()


def get_max_available_timestamp(dataset: DatasetReference) -> datetime.datetime:
    return _resolve_dataset_class(dataset).get_max_available_timestamp()
