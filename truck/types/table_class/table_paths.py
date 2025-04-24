from __future__ import annotations

import typing
import truck
from . import table_base

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')


class TablePaths(table_base.TableBase):
    def get_dir_path(self, warn: bool = True) -> str:
        return truck.ops.paths.get_table_dir(
            source=self.source,
            table=self.name(snake=True),
            warn=warn,
        )

    def get_glob(self, warn: bool = True) -> str:
        return self.get_file_path(glob=True, warn=warn)

    def get_file_path(
        self,
        data_range: typing.Any | None = None,
        glob: bool = False,
        warn: bool = True,
    ) -> str:
        if self.write_range == 'overwrite_all':
            data_range = 'all'
            range_format = None
        else:
            range_format = self.range_format
        return truck.ops.paths.get_table_filepath(
            data_range=data_range,
            range_format=range_format,
            filename_template=self.filename_template,
            table=self.name(snake=True),
            source=self.source,
            parameters=self.parameters,
            glob=glob,
            warn=warn,
        )

    def get_file_paths(
        self, data_ranges: typing.Any, warn: bool = True
    ) -> list[str]:
        return truck.ops.paths.get_table_filepaths(
            data_ranges=data_ranges,
            range_format=self.range_format,
            filename_template=self.filename_template,
            table=self.name(snake=True),
            source=self.source,
            parameters=self.parameters,
            warn=warn,
        )

    def get_file_name(
        self, data_range: typing.Any, *, glob: bool = False
    ) -> str:
        if self.write_range == 'overwrite_all':
            data_range = 'all'
            range_format = None
        else:
            range_format = self.range_format
        return truck.ops.paths.get_table_filename(
            data_range=data_range,
            range_format=range_format,
            filename_template=self.filename_template,
            table=self.name(snake=True),
            source=self.source,
            parameters=self.parameters,
            glob=glob,
        )

    def parse_file_path(self, path: str) -> dict[str, typing.Any]:
        return truck.ops.paths.parse_file_path(
            path=path,
            filename_template=self.filename_template,
            range_format=self.range_format,
        )
