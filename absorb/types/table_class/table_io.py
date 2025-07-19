from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl

from . import table_paths


class TableIO(table_paths.TablePaths):
    def scan(
        self,
        *,
        parameters: dict[str, typing.Any] | None = None,
        scan_kwargs: dict[str, typing.Any] | None = None,
    ) -> pl.LazyFrame:
        import polars as pl

        if scan_kwargs is None:
            scan_kwargs = {}
        try:
            return pl.scan_parquet(self.get_glob(), **scan_kwargs)
        except Exception as e:
            if e.args[0].startswith('expected at least 1 source'):
                raise Exception('no data to load for ' + str(self.name()))
            else:
                raise e

    def load(self, **kwargs: typing.Any) -> pl.DataFrame:
        import polars as pl

        try:
            return self.scan(**kwargs).collect()
        except pl.exceptions.ComputeError as e:
            if e.args[0].startswith('expected at least 1 source'):
                raise Exception('no data to load for ' + str(self.name()))
            else:
                raise e
