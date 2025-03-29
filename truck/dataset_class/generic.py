from __future__ import annotations

from . import download


class Dataset(
    DatasetDownload,
    DatasetIO,
    DatasetPaths,
):
    def __init__(self):
        raise Exception('do not instantiate class, just use the classmethods')
