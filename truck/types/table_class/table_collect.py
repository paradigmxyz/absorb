from __future__ import annotations

import typing
import truck

from . import table_coverage

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import polars as pl


class TableCollect(table_coverage.TableCoverage):
    def collect(
        self, data_range: typing.Any | None = None, overwrite: bool = False
    ) -> None:
        chunk_ranges, paths = self._get_chunk_ranges(data_range, overwrite)

        # collect chunks
        for chunk_range, path in zip(chunk_ranges, paths):
            df = self.collect_chunk(data_range=data_range)
            df.write_parquet(path)

    async def async_collect(
        self, data_range: typing.Any | None = None, overwrite: bool = False
    ) -> None:
        import asyncio

        chunk_ranges, paths = self._get_chunk_ranges(data_range, overwrite)
        coroutines = [
            self._collect_and_save(chunk_range, path)
            for chunk_range, path in zip(chunk_ranges, paths)
        ]
        await asyncio.gather(*coroutines)

    async def _collect_and_save(
        self, data_range: typing.Any | None, path: str
    ) -> None:
        df = await self.async_collect_chunk(data_range=data_range)
        df.write_parquet(path)

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        raise NotImplementedError()

    async def async_collect_chunk(self, data_range: typing.Any) -> pl.DataFrame:
        raise NotImplementedError()

    def _get_chunk_ranges(
        self, data_range: typing.Any | None = None, overwrite: bool = False
    ) -> tuple[list[typing.Any], list[str]]:
        import os

        # get data range
        if data_range is None:
            if overwrite:
                data_range = self.get_available_range()
            else:
                data_range = truck.ops.ranges.get_range_diff(
                    subtract_this=self.get_collected_range(),
                    from_this=self.get_available_range(),
                    range_format=self.range_format,
                )

        # partition range into chunks
        chunk_ranges = truck.ops.ranges.partition_into_chunks(
            data_range, range_format=self.range_format
        )
        paths = [
            self.get_file_path(data_range=chunk_range)
            for chunk_range in chunk_ranges
        ]

        # filter existing
        if not overwrite:
            new_chunk_ranges = []
            new_paths = []
            for chunk_range, path in zip(chunk_ranges, paths):
                if os.path.isfile(path):
                    new_chunk_ranges.append(chunk_range)
                    new_paths.append(path)
            chunk_ranges = new_chunk_ranges
            paths = new_paths

        return chunk_ranges, paths
