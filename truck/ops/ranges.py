from __future__ import annotations

import typing
import truck


def get_range_diff(
    subtract_this: typing.Any,
    from_this: typing.Any,
    range_format: truck.RangeFormat,
) -> typing.Any:
    raise NotImplementedError()


def partition_into_chunks(
    data_range: typing.Any, range_format: truck.RangeFormat
) -> list[typing.Any]:
    raise NotImplementedError()
