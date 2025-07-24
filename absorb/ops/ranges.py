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


def index_is_temporal(index_type: absorb.IndexType | None) -> bool:
    return index_type in [
        'hour',
        'day',
        'week',
        'month',
        'quarter',
        'year',
        'timestamp_range',
    ]


def coverage_to_list(
    coverage: absorb.Coverage,
    index_type: absorb.IndexType,
) -> absorb.ChunkList:
    if isinstance(coverage, list):
        if all(isinstance(item, tuple) for item in coverage) and index_type in [
            'hour',
            'day',
            'week',
            'month',
            'quarter',
            'year',
        ]:
            return [
                subitem
                for item in coverage
                for subitem in coverage_to_list(item, index_type)
            ]
        else:
            return coverage
    elif isinstance(coverage, dict):
        if not isinstance(index_type, dict):
            raise Exception()
        if index_type['type'] == 'multi':
            return _multi_coverage_to_list(coverage, index_type)
        else:
            raise NotImplementedError(
                'using number_range or timestamp_range with interval size'
            )
    elif isinstance(coverage, tuple):
        import tooltime

        start, end = coverage
        if index_type in ['hour', 'day', 'week', 'month', 'quarter', 'year']:
            if not isinstance(index_type, str):  # for mypy
                raise Exception()
            return tooltime.get_intervals(
                start,
                end,
                interval=index_type,
                include_end=True,
            )['start'].to_list()
        elif index_type == 'number':
            return list(range(start, end + 1))
        else:
            raise Exception('cannot use this chunk_type as tuple range')
    else:
        raise Exception('invalid coverage format')


def _multi_coverage_to_list(
    coverage: absorb.Coverage,
    index_type: absorb.MultiIndexType,
) -> absorb.ChunkList:
    import itertools

    if not isinstance(coverage, dict):
        raise Exception()
    keys = list(coverage.keys())
    dims = [
        coverage_to_list(
            coverage=coverage[key],
            index_type=index_type['dims'][key],
        )
        for key in keys
    ]
    return [dict(zip(keys, combo)) for combo in itertools.product(*dims)]


def get_range_diff(
    subtract_this: absorb.Coverage,
    from_this: absorb.Coverage,
    index_type: absorb.IndexType,
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
    non_range_types = [
        'number_list',
        'timestamp',
        'id',
        'id_range',
        'id_list',
    ]

    if (
        isinstance(subtract_this, (list, dict))
        or isinstance(from_this, (list, dict))
        or index_type in non_range_types
    ):
        if not isinstance(subtract_this, list):
            subtract_this = coverage_to_list(subtract_this, index_type)
        if not isinstance(from_this, list):
            from_this = coverage_to_list(from_this, index_type)
        return [item for item in from_this if item not in subtract_this]

    if not isinstance(subtract_this, tuple) or not isinstance(from_this, tuple):
        raise Exception()
    if index_type in [
        'hour',
        'day',
        'week',
        'month',
        'quarter',
        'year',
    ]:
        import datetime
        import tooltime

        # get discrete chunk
        discrete_step: datetime.timedelta | tooltime.DateDelta
        if index_type == 'hour':
            discrete_step = datetime.timedelta(hours=1)
        elif index_type == 'day':
            discrete_step = datetime.timedelta(days=1)
        elif index_type == 'week':
            discrete_step = datetime.timedelta(days=7)
        elif index_type == 'month':
            discrete_step = tooltime.DateDelta(months=1)
        elif index_type == 'quarter':
            discrete_step = tooltime.DateDelta(quarters=1)
        elif index_type == 'year':
            discrete_step = tooltime.DateDelta(years=1)
        else:
            raise Exception('invalid index_type')

        range_list: absorb.Coverage = _get_discrete_closed_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
            discrete_step=discrete_step,
        )
    elif index_type == 'timestamp_range':
        range_list = _get_continuous_closed_open_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
        )
    elif index_type == 'number':
        range_list = _get_discrete_closed_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
            discrete_step=1,
        )
    elif index_type == 'number_range':
        range_list = _get_discrete_closed_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
            discrete_step=1,
        )
    elif isinstance(index_type, absorb.CustomIndexType):
        raise NotImplementedError()
    else:
        raise Exception('invalid index_type')

    if len(range_list) == 0:
        return []
    elif len(range_list) == 1:
        return range_list[0]  # type: ignore
    else:
        return range_list
        # return [
        #     item
        #     for range in range_list
        #     for item in coverage_to_list(range, index_type=index_type)
        # ]


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
    coverage: absorb.Coverage, index_type: absorb.IndexType
) -> absorb.ChunkList:
    return coverage_to_list(coverage=coverage, index_type=index_type)
