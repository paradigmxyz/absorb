from __future__ import annotations

import typing
import absorb

from . import table_coverage

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import polars as pl


class TableCollect(table_coverage.TableCoverage):
    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame | None:
        raise NotImplementedError()

    def collect(
        self,
        data_range: typing.Any | None = None,
        *,
        overwrite: bool = False,
        verbose: int = 1,
        dry: bool = False,
    ) -> None:
        # get collection plan
        chunks = self._get_chunks_to_collect(data_range, overwrite)

        # summarize collection plan
        if verbose >= 1:
            self._summarize_collection_plan(chunks, overwrite, verbose, dry)

        # return early if dry
        if dry:
            return None

        # collect each chunk
        for chunk in chunks:
            self._execute_collect_chunk(chunk, overwrite, verbose)

    def _get_chunks_to_collect(
        self, data_range: typing.Any | None = None, overwrite: bool = False
    ) -> absorb.ChunkList:
        if self.write_range == 'overwrite_all':
            return [None]
        else:
            if data_range is None:
                if overwrite:
                    coverage = self.get_available_range()
                else:
                    coverage = self.get_missing_ranges()
            else:
                coverage = [data_range]
            data_ranges = absorb.ops.coverage_to_list(
                coverage, index_type=self.index_type
            )
            return absorb.ops.ranges.partition_into_chunks(
                data_ranges, index_type=self.index_type
            )

    def _summarize_collection_plan(
        self,
        chunks: absorb.ChunkList,
        overwrite: bool,
        verbose: int,
        dry: bool,
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
        absorb.ops.print_bullet('n_chunks', str(len(chunks)))
        if self.write_range == 'overwrite_all':
            absorb.ops.print_bullet('chunk', '\[entire dataset]')  # noqa
        elif len(chunks) == 1:
            absorb.ops.print_bullet(
                'single chunk',
                absorb.ops.format_chunk(chunks[0], self.index_type),
            )
        elif len(chunks) > 1:
            absorb.ops.print_bullet(
                'min_chunk',
                absorb.ops.format_chunk(chunks[0], self.index_type),
                indent=4,
            )
            absorb.ops.print_bullet(
                'max_chunk',
                absorb.ops.format_chunk(chunks[-1], self.index_type),
                indent=4,
            )
        absorb.ops.print_bullet('overwrite', str(overwrite))
        absorb.ops.print_bullet('output dir', self.get_dir_path())
        absorb.ops.print_bullet(
            'collection start time', str(datetime.datetime.now())
        )
        if len(chunks) == 0:
            print('[already collected]')

        if verbose > 1:
            print()
            absorb.ops.print_bullet(key='chunks', value='')
            for c, chunk in enumerate(chunks):
                absorb.ops.print_bullet(
                    key=None,
                    value=absorb.ops.format_chunk(chunk, self.index_type),
                    number=c + 1,
                    indent=4,
                )

        if dry:
            print('[dry run]')
        if len(chunks) > 0:
            print()

    def _execute_collect_chunk(
        self,
        chunk: typing.Any,
        overwrite: bool,
        verbose: int,
    ) -> None:
        import os

        if verbose >= 1:
            if self.write_range == 'overwrite_all':
                as_str = 'all'
            else:
                as_str = absorb.ops.format_chunk(chunk, self.index_type)
            print('collecting', as_str)
        df = self.collect_chunk(data_range=chunk)
        if df is not None:
            path = self.get_file_path(data_range=chunk, df=df)
            absorb.ops.write_file(df=df, path=path)
        if verbose >= 1 and df is None:
            print('could not collect data for', os.path.basename(path))
