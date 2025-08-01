from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl

import absorb
from . import table_paths


class TableIO(table_paths.TablePaths):
    def scan(
        self,
        *,
        scan_kwargs: dict[str, typing.Any] | None = None,
    ) -> pl.LazyFrame:
        import polars as pl

        if scan_kwargs is None:
            scan_kwargs = {}
        try:
            return pl.scan_parquet(self.get_data_glob(), **scan_kwargs)
        except Exception as e:
            if e.args[0].startswith('expected at least 1 source'):
                raise Exception('no data to load for ' + str(self.full_name()))
            else:
                raise e

    def load(self, **kwargs: typing.Any) -> pl.DataFrame:
        """kwargs are the parameters of Table.scan()"""
        import polars as pl

        try:
            return self.scan(**kwargs).collect()
        except pl.exceptions.ComputeError as e:
            if e.args[0].startswith('expected at least 1 source'):
                raise Exception('no data to load for ' + str(self.full_name()))
            else:
                raise e
