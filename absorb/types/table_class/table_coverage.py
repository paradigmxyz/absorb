from __future__ import annotations

import typing
from . import table_io
import absorb

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import datetime


class TableCoverage(table_io.TableIO):
    def get_available_range(self) -> absorb.Coverage:
        if self.write_range == 'overwrite_all':
            return None
        else:
            raise NotImplementedError()

    def get_collected_range(self) -> absorb.Coverage | None:
        import os
        import glob

        dir_path = self.get_dir_path()
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
            start = self.parse_file_path(files[0])['chunk']
            end = self.parse_file_path(files[-1])['chunk']
            return (start, end)
        else:
            raise Exception()

    def get_missing_ranges(self) -> absorb.Coverage:
        collected_range = self.get_collected_range()
        available_range = self.get_available_range()
        if collected_range is None:
            return [available_range]
        else:
            return absorb.ops.ranges.get_range_diff(
                subtract_this=collected_range,
                from_this=available_range,
                index_type=self.index_type,
            )

    @classmethod
    def is_range_sortable(cls) -> bool:
        return cls.index_type is not None

    def get_min_collected_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()

    def get_max_collected_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()

    def get_min_available_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()

    def get_max_available_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()
