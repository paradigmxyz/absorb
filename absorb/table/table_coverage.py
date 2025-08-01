from __future__ import annotations

import typing
from . import table_io
import absorb

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import datetime


class TableCoverage(table_io.TableIO):
    def get_available_range(self) -> absorb.Coverage | None:
        if self.write_range == 'overwrite_all':
            return None
        else:
            raise NotImplementedError(
                'get_available_range() not implemented for '
                + self.source
                + '.'
                + str(type(self).__name__)
            )

    def get_collected_range(self) -> absorb.Coverage | None:
        import os
        import glob

        dir_path = self.get_table_dir()
        if not os.path.isdir(dir_path):
            return None

        data_glob = self.get_data_glob()
        if self.write_range == 'overwrite_all':
            files = sorted(glob.glob(data_glob))
            if len(files) == 0:
                return None
            elif len(files) == 1:
                import polars as pl

                # for now: only handle timestamp ranges if timestamp present
                schema = self.scan().collect_schema()
                if 'timestamp' in schema.names():
                    df = (
                        self.scan()
                        .select(
                            min_timestamp=pl.col.timestamp.min(),
                            max_timestamp=pl.col.timestamp.max(),
                        )
                        .collect()
                    )
                    return (df['min_timestamp'][0], df['max_timestamp'][0])

                else:
                    return None
            else:
                raise Exception(
                    'too many files, there should only be one parquet file when when overwrite_all=True'
                )
        elif self.is_range_sortable():
            files = sorted(glob.glob(data_glob))
            if len(files) == 0:
                return None
            start = self.parse_chunk_path(files[0])['chunk']
            end = self.parse_chunk_path(files[-1])['chunk']
            return (start, end)
        else:
            raise Exception()

    def get_missing_ranges(self) -> absorb.Coverage:
        if self.write_range == 'overwrite_all':
            raise Exception(
                'get_missing_ranges() does not apply to tables that use overwrite_all'
            )
        available_range = self.get_available_range()
        if available_range is None:
            raise Exception('get_available_range() not properly implemented')

        collected_range = self.get_collected_range()
        if collected_range is None:
            return available_range
        else:
            chunk_size = self.get_chunk_size()
            if chunk_size is None:
                raise Exception(
                    'ranges computations require chunk_size to be set'
                )
            return absorb.ops.get_range_diff(
                subtract_this=collected_range,
                from_this=available_range,
                chunk_size=chunk_size,
                boundary_type=self.boundary_type,
            )

    def is_range_sortable(self) -> bool:
        return self.get_chunk_size() is not None

    def ready_for_update(self) -> bool:
        """used for periodically updating datasets that have no get_available_range()"""
        import datetime

        # get last update time
        last_update_time = self.get_last_update_time()
        if last_update_time is None:
            return True

        # get min update latency
        update_latency = datetime.timedelta(self.get_update_latency())

        # return whether now is past the last update time + min update_latency
        return datetime.datetime.now() > last_update_time + update_latency

    def get_last_update_time(self) -> datetime.datetime | None:
        """
        there are a few options for how to do this
        - the max timestamp in the table
        - the time that the dataset was collected
        - can decide based on whether is temporal and whether write_range=all
        """
        if self.get_index_type() == 'temporal':
            return self.get_max_collected_timestamp()
        else:
            import glob

            files = sorted(glob.glob(self.get_data_glob()))
            if len(files) == 0:
                return None
            parsed = self.parse_chunk_path(files[-1])
            if 'chunk' in parsed:
                try:
                    return absorb.ops.parse_raw_datetime(parsed['chunk'])
                except ValueError:
                    raise Exception('cannot parse last update time from chunk')
            else:
                raise Exception('cannot parse last update time from chunk')

    def get_min_collected_timestamp(self) -> datetime.datetime | None:
        import polars as pl

        return self.scan().select(pl.col.timestamp.min()).collect().item()  # type: ignore

    def get_max_collected_timestamp(self) -> datetime.datetime | None:
        import polars as pl

        return self.scan().select(pl.col.timestamp.max()).collect().item()  # type: ignore

    def get_collected_timestamp_range(
        self,
    ) -> tuple[datetime.datetime | None, datetime.datetime | None]:
        import polars as pl

        return (
            self.scan()
            .select(pl.col.timestamp.min(), pl.col.timestamp.max())
            .collect()
            .row(0)
        )
