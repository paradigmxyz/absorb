from __future__ import annotations

import typing
import absorb

from . import table_coverage

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import datetime
    import polars as pl
    from typing_extensions import NotRequired

    class ChunkResultSummary(typing.TypedDict):
        success: bool
        paths: NotRequired[list[str]]
        bytes_in_memory: NotRequired[int]
        bytes_on_disk: NotRequired[int]
        n_rows: NotRequired[int]


class TableCollect(table_coverage.TableCoverage):
    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        raise NotImplementedError()

    def is_collected(self) -> bool:
        """return True if any data files exist"""
        import glob

        data_glob = self.get_data_glob()
        return len(glob.glob(data_glob)) > 0

    def collect(
        self,
        data_range: typing.Any | None = None,
        *,
        overwrite: bool = False,
        verbose: int = 1,
        dry: bool = False,
    ) -> None:
        import datetime

        self._check_ready_to_collect()

        # get collection plan
        chunks = self._get_chunks_to_collect(data_range, overwrite)

        # summarize collection plan
        start = datetime.datetime.now()
        if verbose >= 1:
            self._summarize_collect_plan(chunks, overwrite, verbose, dry, start)

        # return early if dry
        if dry:
            return None

        # create table directory
        self.setup_table_dir()

        # collect each chunk
        chunk_summaries = [
            self._execute_collect_chunk(chunk, overwrite, verbose)
            for chunk in chunks
        ]

        # summarize collection
        self._summarize_collected_data(chunk_summaries, start, verbose)

    def _check_ready_to_collect(self) -> None:
        import os

        # check package dependencies
        missing_packages = self.get_missing_packages()
        if len(missing_packages) > 0:
            raise Exception(
                'required packages not installed: '
                + ', '.join(missing_packages)
            )

        # check credentials
        missing_credentials = self.get_missing_credentials()
        if len(missing_credentials) > 0:
            raise Exception(
                'required credentials not found: '
                + ', '.join(missing_credentials)
            )

    def _get_chunks_to_collect(
        self, data_range: absorb.Coverage | None = None, overwrite: bool = False
    ) -> list[absorb.Chunk]:
        if self.write_range == 'overwrite_all':
            if overwrite:
                return [None]
            available_range = self.get_available_range()
            collected_range = self.get_collected_range()
            if available_range is not None:
                # if available_range exists, use it to decide whether to collect
                if available_range == collected_range:
                    return []
                else:
                    if isinstance(available_range, tuple):
                        return [available_range]
                    elif isinstance(available_range, list):
                        return list(available_range)
                    elif isinstance(available_range, dict):
                        return [available_range]
                    elif available_range is None:
                        return [None]
                    else:
                        raise Exception('invalid available range')
            elif (
                collected_range is not None
                and self.get_index_type() == 'temporal'
            ):
                # if temporal, check if ready for update
                if self.ready_for_update():
                    return [None]
                else:
                    return []
            elif self.is_collected():
                # if already collected, do not collect again
                return []
            else:
                # if not yet collected, collect entire dataset
                return [None]

        else:
            chunk_size = self.get_chunk_size()
            if chunk_size is None:
                raise Exception(
                    'index type is required if not using overwrite_all'
                )

            # get coverage range for collection
            coverage: absorb.Coverage
            if data_range is not None:
                if overwrite:
                    coverage = data_range
                else:
                    collected_range = self.get_collected_range()
                    if collected_range is None:
                        coverage = data_range
                    else:
                        coverage = absorb.ops.get_range_diff(
                            subtract_this=collected_range,
                            from_this=data_range,
                            chunk_size=chunk_size,
                            boundary_type=self.boundary_type,
                        )
            else:
                if overwrite:
                    available_range = self.get_available_range()
                    if available_range is None:
                        raise Exception(
                            'get_available_range() not properly implemented'
                        )
                    coverage = available_range
                else:
                    coverage = self.get_missing_ranges()

            # split each range into chunk
            return absorb.ops.partition_into_chunks(coverage, chunk_size)

    def _summarize_collect_plan(
        self,
        chunks: list[absorb.Chunk],
        overwrite: bool,
        verbose: int,
        dry: bool,
        start: datetime.datetime,
    ) -> None:
        import datetime
        import rich

        rich.print(
            '[bold][green]collecting dataset:[/green] [white]'
            + self.full_name()
            + '[/white][/bold]'
        )
        absorb.ops.print_bullet('n_chunks', str(len(chunks)))
        if self.write_range == 'overwrite_all':
            absorb.ops.print_bullet('chunk', '\\' + '[entire dataset]')  # noqa
        elif len(chunks) == 1:
            absorb.ops.print_bullet(
                'single chunk',
                absorb.ops.format_chunk(chunks[0], self.get_chunk_size()),
            )
        elif len(chunks) > 1:
            absorb.ops.print_bullet(
                'min_chunk',
                absorb.ops.format_chunk(chunks[0], self.get_chunk_size()),
                indent=4,
            )
            absorb.ops.print_bullet(
                'max_chunk',
                absorb.ops.format_chunk(chunks[-1], self.get_chunk_size()),
                indent=4,
            )
        absorb.ops.print_bullet('overwrite', str(overwrite))
        absorb.ops.print_bullet('output dir', self.get_table_dir())
        absorb.ops.print_bullet('collection start time', str(start))
        if len(chunks) == 0:
            print('[already collected]')

        if verbose > 1:
            print()
            absorb.ops.print_bullet(key='chunks', value='')
            chunk_size = self.get_chunk_size()
            for c, chunk in enumerate(chunks):
                chunk_str = absorb.ops.format_chunk(chunk, chunk_size)
                absorb.ops.print_bullet(
                    key=None, value=chunk_str, number=c + 1, indent=4
                )

        if dry:
            print('[dry run]')
        elif len(chunks) > 0:
            print()

    def _summarize_collected_data(
        self,
        summaries: list[ChunkResultSummary],
        start_time: datetime.datetime,
        verbose: int,
    ) -> None:
        import datetime
        import toolstr

        if not verbose:
            return

        # aggregate totals
        total_rows = 0
        total_disk_bytes = 0
        total_memory_bytes = 0
        n_success = 0
        for summary in summaries:
            if summary['success']:
                total_rows += summary['n_rows']
                total_disk_bytes += summary['bytes_on_disk']
                total_memory_bytes += summary['bytes_in_memory']
                n_success += 1
        n_fail = len(summaries) - n_success

        # compute timings
        end_time = datetime.datetime.now()
        total_time = (end_time - start_time).total_seconds()
        total_time = (end_time - start_time).total_seconds()

        # print summary

        import rich

        print()
        if n_fail > 0:
            start_tag = '[red]'
            end_tag = '[/red]'
            symbol_color = 'red'
        else:
            start_tag = ''
            end_tag = ''
            symbol_color = 'green'
        rich.print(
            '[bold][green]'
            + start_tag
            + 'done collecting:'
            + end_tag
            + '[/green] [white]'
            + self.full_name()
            + '[/white][/bold]'
        )
        absorb.ops.print_bullet(
            'successful chunks',
            str(n_success) + ' / ' + str(len(summaries)),
            symbol_color=symbol_color,
        )
        absorb.ops.print_bullet(
            'collection end time',
            '  ' + str(end_time),
            symbol_color=symbol_color,
        )
        absorb.ops.print_bullet(
            'collection total time',
            toolstr.format(total_time, decimals=2) + ' seconds',
            symbol_color=symbol_color,
        )
        absorb.ops.print_bullet(
            'rows collected',
            toolstr.format(total_rows),
            symbol_color=symbol_color,
        )
        absorb.ops.print_bullet(
            'bytes collected',
            toolstr.format_nbytes(total_disk_bytes)
            + ' on disk, '
            + toolstr.format_nbytes(total_memory_bytes)
            + ' in memory ('
            + toolstr.format(total_memory_bytes / total_disk_bytes, decimals=2)
            + 'x compression)',
            symbol_color=symbol_color,
        )

    def _execute_collect_chunk(
        self,
        chunk: absorb.Chunk,
        overwrite: bool,
        verbose: int,
    ) -> ChunkResultSummary:
        import glob
        import os

        # print summary
        if verbose >= 1:
            if self.write_range == 'overwrite_all':
                as_str = 'all'
                print('[collecting entire dataset]')
            else:
                as_str = absorb.ops.format_chunk(chunk, self.get_chunk_size())
                print('[collecting', as_str + ']')

        # collect chunk
        data = self.collect_chunk(chunk=chunk)

        # validate chunk
        self.validate_chunk(chunk=chunk, data=data)

        # write file
        if data is None:
            chunk_summary: ChunkResultSummary = {'success': False}
        elif self.chunk_datatype == 'dataframe':
            import polars as pl

            if not isinstance(data, pl.DataFrame):
                raise Exception(
                    'collected data is not a DataFrame: ' + str(type(data))
                )
            path = self.get_chunk_path(chunk=chunk, df=data)
            absorb.ops.write_file(df=data, path=path)

            # delete other files if write_range=overwrite_all
            if self.write_range == 'overwrite_all':
                for other_path in glob.glob(self.get_data_glob()):
                    if other_path != path:
                        print('removing old data', other_path)
                        os.remove(other_path)

            chunk_summary = {
                'success': True,
                'paths': [path],
                'bytes_in_memory': int(data.estimated_size()),
                'bytes_on_disk': os.path.getsize(path),
                'n_rows': data.shape[0],
            }
        else:
            raise Exception()

        # print post-summary
        if verbose >= 1 and data is None:
            print('could not collect data for', str(chunk))

        return chunk_summary

    def validate_chunk(
        self, chunk: absorb.Chunk, data: absorb.ChunkResult | None
    ) -> None:
        import os
        import polars as pl

        if data is None:
            return

        if self.chunk_datatype == 'dataframe':
            if not isinstance(data, pl.DataFrame):
                raise Exception(
                    'collected data is not a DataFrame: ' + str(type(data))
                )
            assert dict(data.schema) == self.get_schema(), (
                'collected data does not match schema: '
                + str(dict(data.schema))
                + ' != '
                + str(self.get_schema())
            )
        elif self.chunk_datatype == 'files':
            if not isinstance(data, dict) or data.get('type') != 'files':
                raise Exception(
                    'collected data is not a path dict: ' + str(type(data))
                )
            for path in data['paths']:
                assert os.path.exists(path), (
                    'collected data does not exist: ' + path
                )
                file_schema = pl.scan_parquet(path).collect_schema()
                assert dict(file_schema) == self.get_schema(), (
                    'collected data does not match schema: '
                    + str(dict(file_schema))
                    + ' != '
                    + str(self.get_schema())
                )
        else:
            raise Exception('invalid data format: ' + str(type(data)))
