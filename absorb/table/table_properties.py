from __future__ import annotations

import typing

import absorb
from . import table_base


class TableProperties(table_base.TableBase):
    def __getattribute__(self, name: str) -> typing.Any:
        if name in [
            'index_type',
            'index_column',
            'chunk_size',
            'row_precision',
        ]:
            raise Exception(
                'use self.get_' + name + '() instead of self.' + name
            )
        return super().__getattribute__(name)

    def get_index_type(self) -> absorb.IndexType | None:
        if type(self).index_type is not None:
            return type(self).index_type

        # attempt to determine index_type from chunk_size
        chunk_size = self.get_chunk_size()
        if chunk_size is None:
            pass
        elif chunk_size in absorb.ops.temporal_intervals:
            return 'temporal'
        elif isinstance(chunk_size, int):
            return 'numerical'
        elif isinstance(chunk_size, dict):
            raise NotImplementedError('chunk_size as dict is not implemented')
        else:
            raise Exception('invalid type for chunk_size')

        # attempt to determine index_type from row_precision
        if self.get_row_precision() in absorb.ops.temporal_intervals:
            return 'temporal'

        raise Exception('cannot determine index_type')

    def get_index_column(self) -> str | tuple[str, ...] | None:
        if type(self).index_column is not None:
            return type(self).index_column

        index_type = self.get_index_type()
        if index_type == 'temporal':
            return 'timestamp'
        elif index_type in ['numerical', 'id', 'no_index', None]:
            raise Exception('cannot determine index column')
        elif isinstance(index_type, dict):
            raise NotImplementedError('index_type as dict is not implemented')
        else:
            raise Exception('invalid index type: ' + str(index_type))

    def get_chunk_size(self) -> absorb.ChunkSize | None:
        return type(self).chunk_size

    def get_row_precision(self) -> typing.Any | None:
        return type(self).row_precision
