from __future__ import annotations

import typing
import datetime

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

    def get_update_latency(self) -> int | float:
        import tooltime

        class_update_latency = type(self).update_latency
        chunk_size = self.get_chunk_size()
        row_precision = self.get_row_precision()
        if isinstance(class_update_latency, str):
            return tooltime.timelength_to_seconds(class_update_latency)
        elif isinstance(class_update_latency, int):
            return class_update_latency
        elif isinstance(class_update_latency, float):
            if isinstance(row_precision, str):
                seconds = tooltime.timelength_to_seconds('1 ' + row_precision)
                return class_update_latency * seconds
            if isinstance(chunk_size, str):
                seconds = tooltime.timelength_to_seconds('1 ' + chunk_size)
                return class_update_latency * seconds
            raise Exception(
                'if using a float update_latency, must have chunk_size or row_precision'
            )
        elif class_update_latency is None:
            if isinstance(chunk_size, str):
                return tooltime.timelength_to_seconds('1 ' + chunk_size)
            if isinstance(row_precision, str):
                return tooltime.timelength_to_seconds('1 ' + row_precision)
            raise Exception('could not determine update latency')
        else:
            raise Exception('invalid format for class update_latency')
