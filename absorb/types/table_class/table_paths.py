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
        chunk: absorb.Chunk | None = None,
        glob: bool = False,
        warn: bool = True,
        df: pl.DataFrame | None = None,
    ) -> str:
        if self.write_range == 'overwrite_all':
            if glob:
                chunk = None
            else:
                if df is not None:
                    if 'timestamp' in df.columns:
                        chunk = df['timestamp'].max()
                    else:
                        chunk = 'all'
                else:
                    raise Exception('must specify range')
        return absorb.ops.paths.get_table_filepath(
            chunk=chunk,
            index_type=self.index_type,
            filename_template=self.filename_template,
            table=self.name(),
            source=self.source,
            parameters=self.parameters,
            glob=glob,
            warn=warn,
        )

    def get_file_paths(
        self, chunks: absorb.Coverage, warn: bool = True
    ) -> list[str]:
        return absorb.ops.paths.get_table_filepaths(
            chunks=chunks,
            index_type=self.index_type,
            filename_template=self.filename_template,
            table=self.name(),
            source=self.source,
            parameters=self.parameters,
            warn=warn,
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
