from __future__ import annotations

import typing
import absorb

if typing.TYPE_CHECKING:
    import datetime
    from typing import TypeVar, Protocol

    class SupportsComparison(Protocol):
        def __lt__(self, other: object) -> bool: ...
        def __le__(self, other: object) -> bool: ...
        def __gt__(self, other: object) -> bool: ...
        def __ge__(self, other: object) -> bool: ...
        def __eq__(self, other: object) -> bool: ...

    # _T = TypeVar('_T', int, datetime.datetime, bound=SupportsComparison)
    _T = TypeVar('_T', bound=SupportsComparison)


temporal_intervals = ['hour', 'day', 'week', 'month', 'quarter', 'year']


def get_range_diff(
    subtract_this: absorb.Coverage,
    from_this: absorb.Coverage,
    boundary_type: typing.Literal['closed', 'semiopen'],
    chunk_size: absorb.ChunkSize | None = None,
) -> absorb.Coverage:
    """
    subtraction behaves differently depending on range format
    - mainly, index_type is discrete-closed or continuous-semiopen or other
    - some of these cases will have equivalent outcomes
        - handling them separately keeps maximum clarity + robustness

                                           fs         fe
    original interval                      |----------|
    16 cases of subtraction    1.  |----|
                               2.  |-------|
                               3.  |------------|
                               4.  |------------------|
                               5.  |------------------------|
                               6.          |
                               7.          |------|
                               8.          |----------|
                               9.          |---------------|
                               10.             |
                               11.             |----|
                               12.             |------|
                               13.             |-----------|
                               14.                    |
                               15.                    |-----|
                               16.                        |----|
                                                          ss   se

    if fs == fe
                                            |
                                1.    |--|
                                2.    |-----|
                                3.    |--------|
                                4.          |
                                5.          |--|
                                6.             |--|
    """
    # convert to lists of tuples
    if isinstance(from_this, list):
        pass
    elif isinstance(from_this, tuple):
        from_this = [from_this]
    elif isinstance(from_this, dict):
        raise NotImplementedError('CustomCoverage not supported for from_this')
    else:
        raise Exception('invalid from_this format')
    if isinstance(subtract_this, list):
        pass
    elif isinstance(subtract_this, tuple):
        subtract_this = [subtract_this]
    elif isinstance(subtract_this, dict):
        raise NotImplementedError(
            'CustomCoverage not supported for subtract_this'
        )
    else:
        raise Exception('invalid subtract_this format')

    # return early if from_this is empty
    if len(from_this) == 0:
        return []

    # infer chunk_size if not provided
    if chunk_size is None:
        if isinstance(from_this[0][0], int):
            chunk_size = 1
        elif isinstance(from_this[0][0], datetime.datetime):
            chunk_size = 'day'
        else:
            raise Exception('cannot infer chunk_size from from_this')

    # subtract each entry of subtract this from the entries of from_this
    output: list[tuple[typing.Any, typing.Any]] = from_this
    for sub_subtract_this in subtract_this:
        new_output = []
        for sub_from_this in output:
            new_output.extend(
                _subtract_tuples(
                    subtract_this=sub_subtract_this,
                    from_this=sub_from_this,
                    boundary_type=boundary_type,
                    chunk_size=chunk_size,
                )
            )
        output = new_output

    return output


def _subtract_tuples(
    subtract_this: tuple[_T, _T],
    from_this: tuple[_T, _T],
    chunk_size: absorb.ChunkSize,
    boundary_type: typing.Literal['closed', 'open', 'semiopen'],
) -> list[tuple[_T, _T]]:
    # get discrete_step
    discrete_step: typing.Any
    if chunk_size in temporal_intervals:
        import datetime
        import tooltime

        if chunk_size == 'hour':
            discrete_step = datetime.timedelta(hours=1)
        elif chunk_size == 'day':
            discrete_step = datetime.timedelta(days=1)
        elif chunk_size == 'week':
            discrete_step = datetime.timedelta(days=7)
        elif chunk_size == 'month':
            discrete_step = tooltime.DateDelta(months=1)
        elif chunk_size == 'quarter':
            discrete_step = tooltime.DateDelta(quarters=1)
        elif chunk_size == 'year':
            discrete_step = tooltime.DateDelta(years=1)
        else:
            raise Exception('invalid chunk_size')
    elif isinstance(chunk_size, int):
        discrete_step = 1
    elif isinstance(chunk_size, dict):
        raise NotImplementedError('CustomChunkSize not supported')
    else:
        raise Exception('invalid chunk_size')

    if boundary_type == 'closed':
        return _get_discrete_closed_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
            discrete_step=discrete_step,
        )
    elif boundary_type == 'semiopen':
        return _get_continuous_closed_open_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
        )
    else:
        raise Exception('invalid boundary_type: ' + str(boundary_type))


def _get_discrete_closed_range_diff(
    subtract_this: tuple[_T, _T],
    from_this: tuple[_T, _T],
    discrete_step: typing.Any,
) -> list[tuple[_T, _T]]:
    s_start, s_end = subtract_this
    f_start, f_end = from_this

    # validity checks
    if s_start > s_end:
        raise Exception('invalid interval, start must be <= end')
    if f_start > f_end:
        raise Exception('invalid interval, start must be <= end')

    # 6 possible cases when f_start == f_end
    if f_start == f_end:
        if s_start < f_start and s_end < f_start:
            # case 1
            return [(f_start, f_end)]
        elif s_start < f_start and s_end == f_start:
            # case 2
            return []
        elif s_start < f_start and s_end > f_start:
            # case 3
            return []
        elif s_start == f_start and s_end == f_start:
            # case 4
            return []
        elif s_start == f_start and s_end > f_start:
            # case 5
            return []
        elif s_start > f_start and s_end > f_start:
            # case 6
            return [(f_start, f_end)]
        else:
            raise Exception()

    # 16 possible cases when f_start < f_end
    if s_start < f_start and s_end < f_start:
        # case 1
        return [(f_start, f_end)]
    elif s_start < f_start and s_end == f_start:
        # case 2
        return [(s_end + discrete_step, f_end)]
    elif s_start < f_start and s_end < f_end:
        # case 3
        return [(s_end + discrete_step, f_end)]
    elif s_start < f_start and s_end == f_end:
        # case 4
        return []
    elif s_start < f_start and s_end > f_end:
        # case 5
        return []
    elif s_start == f_start and s_end == f_start:
        # case 6
        return [(s_end + discrete_step, f_end)]
    elif s_start == f_start and s_end < f_end:
        # case 7
        return [(s_end + discrete_step, f_end)]
    elif s_start == f_start and s_end == f_end:
        # case 8
        return []
    elif s_start == f_start and s_end > f_end:
        # case 9
        return []
    elif s_start < f_end and s_end == s_start:
        # case 10
        return [
            (f_start, s_start - discrete_step),
            (s_end + discrete_step, f_end),
        ]
    elif s_start < f_end and s_end < f_end:
        # case 11
        return [
            (f_start, s_start - discrete_step),
            (s_end + discrete_step, f_end),
        ]
    elif s_start < f_end and s_end == f_end:
        # case 12
        return [(f_start, s_start - discrete_step)]
    elif s_start < f_end and s_end > f_end:
        # case 13
        return [(f_start, s_start - discrete_step)]
    elif s_start == f_end and s_end == f_end:
        # case 14
        return [(f_start, s_start - discrete_step)]
    elif s_start == f_end and s_end > f_end:
        # case 15
        return [(f_start, s_start - discrete_step)]
    elif s_start > f_end and s_end > f_start:
        # case 16
        return [(f_start, f_end)]
    else:
        raise Exception()


def _get_continuous_closed_open_range_diff(
    subtract_this: tuple[_T, _T], from_this: tuple[_T, _T]
) -> list[tuple[_T, _T]]:
    s_start, s_end = subtract_this
    f_start, f_end = from_this

    # validity checks
    if s_start >= s_end:
        raise Exception('invalid interval, start must be < end')
    if f_start >= f_end:
        raise Exception('invalid interval, start must be < end')

    # 16 possible cases
    if s_start < f_start and s_end < f_start:
        # case 1
        return [(f_start, f_end)]
    elif s_start < f_start and s_end == f_start:
        # case 2
        return [(f_start, f_end)]
    elif s_start < f_start and s_end < f_end:
        # case 3
        return [(s_end, f_end)]
    elif s_start < f_start and s_end == f_end:
        # case 4
        return []
    elif s_start < f_start and s_end > f_end:
        # case 5
        return []
    elif s_start == f_start and s_end == f_start:
        # case 6
        raise Exception('s_start should not equal s_end')
    elif s_start == f_start and s_end < f_end:
        # case 7
        return [(s_end, f_end)]
    elif s_start == f_start and s_end == f_end:
        # case 8
        return []
    elif s_start == f_start and s_end > f_end:
        # case 9
        return []
    elif s_start < f_end and s_end == s_start:
        # case 10
        raise Exception('s_start should not equal s_end')
    elif s_start < f_end and s_end < f_end:
        # case 11
        return [(f_start, s_start), (s_end, f_end)]
    elif s_start < f_end and s_end == f_end:
        # case 12
        return [(f_start, s_start)]
    elif s_start < f_end and s_end > f_end:
        # case 13
        return [(f_start, s_start)]
    elif s_start == f_end and s_end == f_end:
        # case 14
        raise Exception('s_start should not equal s_end')
    elif s_start == f_end and s_end > f_end:
        # case 15
        return [(f_start, f_end)]
    elif s_start > f_end and s_end > f_start:
        # case 16
        return [(f_start, f_end)]
    else:
        raise Exception()


def partition_into_chunks(
    coverage: absorb.Coverage, chunk_size: absorb.ChunkSize
) -> list[absorb.Chunk]:
    if isinstance(coverage, list):
        return [
            subitem
            for item in coverage
            for subitem in partition_into_chunks(item, chunk_size)
        ]
    elif isinstance(coverage, tuple):
        import tooltime

        start, end = coverage
        if chunk_size in temporal_intervals:
            return tooltime.get_intervals(
                start,
                end,
                interval=typing.cast(str, chunk_size),
                include_end=True,
            )['start'].to_list()
        elif isinstance(chunk_size, int):
            if not isinstance(start, int) or not isinstance(end, int):
                raise Exception(
                    'start and end must be integers for int chunk_size'
                )
            return list(range(start, end + 1, chunk_size))
        else:
            raise Exception('cannot use this chunk_type as tuple range')
    else:
        raise Exception('invalid coverage format')
