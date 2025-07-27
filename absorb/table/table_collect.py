from __future__ import annotations

import typing
import absorb

from . import table_coverage

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import polars as pl


class TableCollect(table_coverage.TableCoverage):
    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkData | None:
        raise NotImplementedError()

    def is_collected(self) -> bool:
        import glob

        data_glob = self.get_glob()
        return len(glob.glob(data_glob)) > 0

    def collect(
        self,
        data_range: typing.Any | None = None,
        *,
        overwrite: bool = False,
        verbose: int = 1,
        dry: bool = False,
    ) -> None:
        self._check_ready_to_collect()

        # get collection plan
        chunks = self._get_chunks_to_collect(data_range, overwrite)

        # summarize collection plan
        if verbose >= 1:
            self._summarize_collection_plan(chunks, overwrite, verbose, dry)

        # return early if dry
        if dry:
            return None

        # create table directory
        self.setup_table_dir()

        # collect each chunk
        for chunk in chunks:
            self._execute_collect_chunk(chunk, overwrite, verbose)

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
            chunk_size = self.chunk_size
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

    def _summarize_collection_plan(
        self,
        chunks: list[absorb.Chunk],
        overwrite: bool,
        verbose: int,
        dry: bool,
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
                absorb.ops.format_chunk(chunks[0], self.chunk_size),
            )
        elif len(chunks) > 1:
            absorb.ops.print_bullet(
                'min_chunk',
                absorb.ops.format_chunk(chunks[0], self.chunk_size),
                indent=4,
            )
            absorb.ops.print_bullet(
                'max_chunk',
                absorb.ops.format_chunk(chunks[-1], self.chunk_size),
                indent=4,
            )
        absorb.ops.print_bullet('overwrite', str(overwrite))
        absorb.ops.print_bullet('output dir', self.get_table_dir())
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
                    value=absorb.ops.format_chunk(chunk, self.chunk_size),
                    number=c + 1,
                    indent=4,
                )

        if dry:
            print('[dry run]')
        if len(chunks) > 0:
            print()

    def _execute_collect_chunk(
        self,
        chunk: absorb.Chunk,
        overwrite: bool,
        verbose: int,
    ) -> None:
        import os

        # print summary
        if verbose >= 1:
            if self.write_range == 'overwrite_all':
                as_str = 'all'
            else:
                as_str = absorb.ops.format_chunk(chunk, self.chunk_size)
            print('collecting', as_str)

        # collect chunk
        data = self.collect_chunk(chunk=chunk)

        # validate chunk
        self.validate_chunk(chunk=chunk, data=data)

        # write file
        if self.chunk_datatype == 'dataframe' and data is not None:
            import polars as pl

            if not isinstance(data, pl.DataFrame):
                raise Exception(
                    'collected data is not a DataFrame: ' + str(type(data))
                )
            path = self.get_file_path(chunk=chunk, df=data)
            absorb.ops.write_file(df=data, path=path)

        # print post-summary
        if verbose >= 1 and data is None:
            print('could not collect data for', str(chunk))

    def validate_chunk(
        self, chunk: absorb.Chunk, data: absorb.ChunkData | None
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
