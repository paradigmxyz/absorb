from __future__ import annotations

import typing
import absorb

if typing.TYPE_CHECKING:
    import datetime

    _T = typing.TypeVar('_T', int, datetime.datetime)


def chunk_coverage_to_list(
    coverage: absorb.Coverage,
    chunk_format: absorb.ChunkFormat,
) -> absorb.ChunkList:
    if isinstance(coverage, list):
        return coverage
    elif isinstance(coverage, dict):
        import itertools

        if not isinstance(chunk_format, dict):
            raise Exception()

        keys = list(coverage.keys())
        dims = [
            chunk_coverage_to_list(
                coverage=coverage[key],
                chunk_format=chunk_format[key],
            )
            for key in keys
        ]
        return [dict(zip(keys, combo)) for combo in itertools.product(*dims)]
    elif isinstance(coverage, tuple):
        import tooltime

        start, end = coverage
        if chunk_format in ['hour', 'day', 'week', 'month', 'quarter', 'year']:
            if not isinstance(chunk_format, str):  # for mypy
                raise Exception()
            return tooltime.get_intervals(
                start,
                end,
                interval=chunk_format,
                include_end=True,
            )['start'].to_list()
        elif chunk_format == 'number':
            return list(range(start, end + 1))
        else:
            raise Exception('cannot use this chunk_type as tuple range')
    else:
        raise Exception('invalid coverage format')


def get_range_diff(
    subtract_this: absorb.Coverage,
    from_this: absorb.Coverage,
    chunk_format: absorb.ChunkFormat,
) -> absorb.Coverage:
    """
    subtraction behaves differently depending on range format
    - mainly, chunk_format is discrete-closed or continuous-semiopen or other
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
    non_range_types = [
        'number_list',
        'timestamp',
        'name',
        'name_range',
        'name_list',
    ]

    if (
        isinstance(subtract_this, (list, dict))
        or isinstance(from_this, (list, dict))
        or chunk_format in non_range_types
    ):
        if not isinstance(subtract_this, list):
            subtract_this = chunk_coverage_to_list(subtract_this, chunk_format)
        if not isinstance(from_this, list):
            from_this = chunk_coverage_to_list(from_this, chunk_format)
        return [item for item in from_this if item not in subtract_this]

    if not isinstance(subtract_this, tuple) or not isinstance(from_this, tuple):
        raise Exception()
    if chunk_format in [
        'hour',
        'day',
        'week',
        'month',
        'quarter',
        'year',
    ]:
        import datetime

        # get discrete chunk
        if chunk_format == 'hour':
            discrete_step = datetime.timedelta(hours=1)
        elif chunk_format == 'day':
            discrete_step = datetime.timedelta(days=1)
        elif chunk_format == 'week':
            discrete_step = datetime.timedelta(days=7)
        elif chunk_format == 'month':
            raise NotImplementedError()
        elif chunk_format == 'quarter':
            raise NotImplementedError()
        elif chunk_format == 'year':
            raise NotImplementedError()
        else:
            raise Exception('invalid chunk_format')

        return _get_discrete_closed_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
            discrete_step=discrete_step,
        )
    elif chunk_format == 'timestamp_range':
        return _get_continuous_closed_open_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
        )
    elif chunk_format == 'number':
        return _get_discrete_closed_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
            discrete_step=1,
        )
    elif chunk_format == 'number_range':
        return _get_discrete_closed_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
            discrete_step=1,
        )
    elif isinstance(chunk_format, absorb.CustomChunkFormat):
        raise NotImplementedError()
    else:
        raise Exception('invalid chunk_format')


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
    coverage: absorb.Coverage, chunk_format: absorb.ChunkFormat
) -> absorb.ChunkList:
    return chunk_coverage_to_list(coverage=coverage, chunk_format=chunk_format)
