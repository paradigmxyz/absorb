from __future__ import annotations

import typing
import absorb
from . import table_names

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')
    import polars as pl


class TablePaths(table_names.TableNames):
    def get_table_dir(self, warn: bool = True) -> str:
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
        # special case the chunk if write_range=overwrite_all
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

        # get file path
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

    def setup_table_dir(self) -> None:
        import json
        import os

        # create directory
        table_dir = self.get_table_dir()
        os.makedirs(table_dir, exist_ok=True)

        # set up metadata
        metadata = self.create_table_dict()
        metadata_path = absorb.ops.get_table_metadata_path(
            self.name(), source=self.source
        )
        if os.path.isfile(metadata_path):
            with open(metadata_path, 'r') as f:
                existing_metadata = json.load(f)
            if json.dumps(existing_metadata, sort_keys=True) != json.dumps(
                metadata, sort_keys=True
            ):
                raise ValueError(
                    'metadata for table does not match existing metadata in directory'
                )
        else:
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f)
            if absorb.ops.get_config()['use_git']:
                absorb.ops.git_add_and_commit_file(
                    metadata_path,
                    repo_root=absorb.ops.get_absorb_root(),
                    message='Collect new table metadata: ' + self.full_name(),
                )

    def create_table_dict(self) -> absorb.TableDict:
        """
        Create metadata for the table file.
        """
        return {
            'source_name': self.source,
            'table_version': self.version,
            'table_name': self.name(),
            'table_class': type(self).__module__ + '.' + type(self).__name__,
            'parameters': self.parameters,
        }
