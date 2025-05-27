from __future__ import annotations

import typing
import absorb
from . import table_base

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')
    import polars as pl


class TablePaths(table_base.TableBase):
    def get_dir_path(self, warn: bool = True) -> str:
        return absorb.ops.paths.get_table_dir(
            source=self.source, table=self.name(), warn=warn
        )

    def get_glob(self, warn: bool = True) -> str:
        return self.get_file_path(glob=True, warn=warn)

    def get_file_path(
        self,
        data_range: typing.Any | None = None,
        glob: bool = False,
        warn: bool = True,
        df: pl.DataFrame | None = None,
    ) -> str:
        if self.write_range == 'overwrite_all':
            if glob:
                data_range = None
            else:
                data_range = self._get_overwrite_range(df)
        return absorb.ops.paths.get_table_filepath(
            data_range=data_range,
            index_type=self.index_type,
            filename_template=self.filename_template,
            table=self.name(),
            source=self.source,
            parameters=self.parameters,
            glob=glob,
            warn=warn,
        )

    def _get_overwrite_range(self, df: pl.DataFrame | None) -> typing.Any:
        raise Exception()
        if df is not None:
            return df['timestamp'].max()
        else:
            raise Exception('must specify range')

    def get_file_paths(
        self, data_ranges: typing.Any, warn: bool = True
    ) -> list[str]:
        return absorb.ops.paths.get_table_filepaths(
            data_ranges=data_ranges,
            index_type=self.index_type,
            filename_template=self.filename_template,
            table=self.name(),
            source=self.source,
            parameters=self.parameters,
            warn=warn,
        )

    def get_file_name(
        self,
        data_range: typing.Any,
        *,
        glob: bool = False,
        df: pl.DataFrame | None = None,
    ) -> str:
        if self.write_range == 'overwrite_all':
            if glob:
                data_range = None
            else:
                data_range = self._get_overwrite_range(df)
        return absorb.ops.paths.get_table_filename(
            data_range=data_range,
            index_type=self.index_type,
            filename_template=self.filename_template,
            table=self.name(),
            source=self.source,
            parameters=self.parameters,
            glob=glob,
        )

    def parse_file_path(self, path: str) -> dict[str, typing.Any]:
        if self.write_range == 'overwrite_all':
            index_type = None
        else:
            index_type = self.index_type
        return absorb.ops.paths.parse_file_path(
            path=path,
            filename_template=self.filename_template,
            index_type=index_type,
        )
