from __future__ import annotations

import typing
from . import table_paths

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')

    import datetime


class TableCoverage(table_paths.TablePaths):
    def get_available_range(self) -> typing.Any:
        return None

    def get_collected_range(self) -> typing.Any:
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
                return self.parse_file_path(files[0])['data_range']
            else:
                raise Exception('too many files')
        elif self.is_range_sortable():
            files = sorted(glob.glob(glob_str))
            start = self.parse_file_path(files[0])['data_range']
            end = self.parse_file_path(files[-1])['data_range']
            return [start, end]
        else:
            raise Exception()

    @classmethod
    def is_range_sortable(cls) -> bool:
        return cls.range_format is not None

    def get_min_collected_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()

    def get_max_collected_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()

    def get_min_available_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()

    def get_max_available_timestamp(self) -> datetime.datetime:
        raise NotImplementedError()
