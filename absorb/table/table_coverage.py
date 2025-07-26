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

        glob_str = self.get_glob()
        if self.write_range == 'overwrite_all':
            files = sorted(glob.glob(glob_str))
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

                # parsed: dict[str, typing.Any] = self.parse_file_path(files[0])
                # if 'chunk' in parsed:
                #     return [parsed['chunk']]
                # else:
                #     raise Exception('chunk not in name template')
            else:
                raise Exception('too many files')
        elif self.is_range_sortable():
            files = sorted(glob.glob(glob_str))
            if len(files) == 0:
                return None
            start = self.parse_file_path(files[0])['chunk']
            end = self.parse_file_path(files[-1])['chunk']
            return (start, end)
        else:
            raise Exception()

    def get_missing_ranges(self) -> absorb.Coverage | None:
        collected_range = self.get_collected_range()
        available_range = self.get_available_range()
        if available_range is None:
            return None
        if collected_range is None:
            return [available_range]
        else:
            index_type = self.index_type
            if index_type is None:
                raise Exception(
                    'ranges computations require index_type to be set'
                )
            return absorb.ops.get_range_diff(
                subtract_this=collected_range,
                from_this=available_range,
                index_type=index_type,
            )

    @classmethod
    def is_range_sortable(cls) -> bool:
        return cls.index_type is not None

    def ready_for_update(self) -> bool:
        """used for periodically updating datasets that have no get_available_range()"""
        import datetime
        import tooltime

        # ensure valid for dataset
        if not absorb.ops.index_is_temporal(self.index_type):
            raise Exception(
                'ready_for_update() can only be called if index is temporal'
            )
        if not isinstance(self.index_type, str):
            raise Exception(
                'ready_for_update() can only be called if index_type is a string'
            )

        # get last update time
        last_update_time = self.get_max_collected_timestamp()
        if last_update_time is None:
            return True

        # get min latency
        min_latency_seconds: float | int
        if self.update_latency is None:
            min_latency_seconds = tooltime.timelength_to_seconds(
                '1 ' + self.index_type
            )
        elif isinstance(self.update_latency, str):
            min_latency_seconds = tooltime.timelength_to_seconds(
                self.update_latency
            )
        elif isinstance(self.update_latency, float):
            min_latency_seconds = (
                self.update_latency
                * tooltime.timelength_to_seconds('1 ' + self.index_type)
            )
        else:
            raise Exception('invalid format for update_latency')
        min_latency = datetime.timedelta(seconds=min_latency_seconds)

        # return whether now is past the last update time + min latency
        return datetime.datetime.now() > last_update_time + min_latency

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
