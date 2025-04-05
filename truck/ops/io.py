from __future__ import annotations

import typing

import truck

if typing.TYPE_CHECKING:
    import tooltime
    import polars as pl


def scan(
    dataset: truck.TableReference,
    start_time: tooltime.Timestamp | None = None,
    end_time: tooltime.Timestamp | None = None,
    root_dir: str | None = None,
    flat: bool | None = None,
    extra_kwargs: dict[str, typing.Any] | None = None,
) -> pl.LazyFrame:
    import polars as pl

    cls = truck.resolve_table_class(dataset)

    raise NotImplementedError()

    # # select time range
    # if start_time is None and end_time is None:
    #     glob = cls.get_paths(
    #         year='*', month='*', day='*', flat=flat, root_dir=root_dir
    #     )
    #     globs = [glob]
    # elif start_time is not None and end_time is not None:
    #     globs = cls.get_paths(
    #         start_time=start_time,
    #         end_time=end_time,
    #         flat=flat,
    #         root_dir=root_dir,
    #     )
    # else:
    #     raise Exception()

    # # scan and set types
    # return pl.scan_parquet(globs)


def load(
    dataset: truck.TableReference,
    start_time: tooltime.Timestamp | None = None,
    end_time: tooltime.Timestamp | None = None,
    root_dir: str | None = None,
    flat: bool | None = None,
    extra_kwargs: dict[str, typing.Any] | None = None,
) -> pl.DataFrame:
    return scan(
        dataset=dataset,
        start_time=start_time,
        end_time=end_time,
        root_dir=root_dir,
        flat=flat,
        extra_kwargs=extra_kwargs,
    ).collect()
