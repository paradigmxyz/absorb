from __future__ import annotations

import typing
import truck

from . import table_coverage

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import polars as pl


class TableCollect(table_coverage.TableCoverage):
    #
    # # each table should implement collect_chunk or async_collect_chunk
    #

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame | None:
        raise NotImplementedError()

    async def async_collect_chunk(
        self, data_range: typing.Any
    ) -> pl.DataFrame | None:
        raise NotImplementedError()

    #
    # # the rest of the methods will be the same for every table
    #

    def collect(
        self,
        data_range: typing.Any | None = None,
        *,
        overwrite: bool = False,
        verbose: int = 1,
        dry: bool = False,
    ) -> None:
        chunk_ranges, paths = self._get_chunks_to_collect(data_range, overwrite)
        if verbose >= 1:
            self.summarize_collection_plan(
                chunk_ranges=chunk_ranges,
                paths=paths,
                overwrite=overwrite,
                verbose=verbose,
            )
        if dry:
            return None
        for chunk_range, path in zip(chunk_ranges, paths):
            if verbose >= 1:
                self.summarize_chunk(chunk_range, path)
            df = self.collect_chunk(data_range=chunk_range)
            if df is None:
                if verbose >= 1:
                    import os

                    print('could not collect data for', os.path.basename(path))
            else:
                truck.ops.write_file(df=df, path=path)

    async def async_collect(
        self,
        data_range: typing.Any | None = None,
        overwrite: bool = False,
        verbose: int = 1,
        dry: bool = False,
    ) -> None:
        import asyncio

        chunk_ranges, paths = self._get_chunks_to_collect(data_range, overwrite)
        if verbose >= 1:
            self.summarize_collection_plan(
                chunk_ranges=chunk_ranges,
                paths=paths,
                overwrite=overwrite,
                verbose=verbose,
            )
        if dry:
            return None
        coroutines = [
            self._async_process_chunk(chunk_range, path, verbose)
            for chunk_range, path in zip(chunk_ranges, paths)
        ]
        await asyncio.gather(*coroutines)

    async def _async_process_chunk(
        self, data_range: typing.Any | None, path: str, verbose: int
    ) -> None:
        if verbose >= 1:
            self.summarize_chunk(data_range, path)
        df = await self.async_collect_chunk(data_range=data_range)
        if df is not None:
            truck.ops.write_file(df=df, path=path)
        else:
            if verbose >= 1:
                import os

                print('could not collect data for', os.path.basename(path))

    def summarize_collection_plan(
        self,
        chunk_ranges: list[typing.Any],
        paths: list[str],
        overwrite: bool,
        verbose: int,
    ) -> None:
        import datetime
        import rich

        rich.print(
            '[bold][green]collecting dataset:[/green] [white]'
            + self.source
            + '.'
            + self.name()
            + '[/white][/bold]'
        )
        truck.ops.print_bullet('n_chunks', str(len(chunk_ranges)))
        if self.write_range == 'overwrite_all':
            truck.ops.print_bullet('chunk', '\[entire dataset]')  # noqa
        elif len(chunk_ranges) == 1:
            truck.ops.print_bullet('single chunk', chunk_ranges[0])
        elif len(chunk_ranges) > 1:
            truck.ops.print_bullet('min_chunk', chunk_ranges[0])
            truck.ops.print_bullet('max_chunk', chunk_ranges[-1])
        truck.ops.print_bullet('overwrite', str(overwrite))
        truck.ops.print_bullet('output dir', self.get_dir_path())
        truck.ops.print_bullet(
            'collection start time', str(datetime.datetime.now())
        )
        if len(chunk_ranges) == 0:
            print('[already collected]')
        print()

        if verbose > 1:
            truck.ops.print_bullet(key='chunks', value='')
            for c, chunk_range in enumerate(chunk_ranges):
                truck.ops.print_bullet(
                    key=None,
                    value=truck.ops.format_range(chunk_range),
                    number=c + 1,
                    indent=4,
                )

    def _get_chunks_to_collect(
        self, data_range: typing.Any | None = None, overwrite: bool = False
    ) -> tuple[list[typing.Any], list[str]]:
        import os

        if self.write_range == 'overwrite_all':
            chunk_ranges = [None]
            paths = [self.get_file_path(None)]

        else:
            # get data range
            if data_range is None:
                if overwrite:
                    data_range = [self.get_available_range()]
                else:
                    collected_range = self.get_collected_range()
                    available_range = self.get_available_range()
                    if collected_range is None:
                        data_ranges = [available_range]
                    else:
                        data_ranges = truck.ops.ranges.get_range_diff(
                            subtract_this=collected_range,
                            from_this=available_range,
                            range_format=self.range_format,
                        )
            else:
                data_ranges = [data_range]

            # partition range into chunks
            chunk_ranges = truck.ops.ranges.partition_into_chunks(
                data_ranges, range_format=self.range_format
            )
            paths = [
                self.get_file_path(data_range=chunk_range)
                for chunk_range in chunk_ranges
            ]

        # filter existing
        if not overwrite:
            new_chunk_ranges: list[typing.Any] = []
            new_paths = []
            for chunk_range, path in zip(chunk_ranges, paths):
                if not os.path.isfile(path):
                    new_chunk_ranges.append(chunk_range)
                    new_paths.append(path)
            chunk_ranges = new_chunk_ranges
            paths = new_paths

        return chunk_ranges, paths

    def summarize_chunk(self, data_range: typing.Any, path: str) -> None:
        import os

        print('collecting', os.path.basename(path))
