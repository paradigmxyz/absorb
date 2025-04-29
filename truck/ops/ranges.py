from __future__ import annotations

import typing
import truck

if typing.TYPE_CHECKING:
    import datetime

    _T = typing.TypeVar('_T', int, datetime.datetime)


def get_range_diff(
    subtract_this: typing.Any,
    from_this: typing.Any,
    range_format: truck.RangeFormat,
) -> list[typing.Any]:
    """
    subtraction behaves differently depending on range format
    - mainly, range_format is discrete-closed or continuous-semiopen or other
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
    if range_format == 'date':
        import datetime

        discrete_step = datetime.timedelta(days=1)

        return _get_discrete_closed_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
            discrete_step=discrete_step,
        )
    elif range_format == 'date_range':
        return _get_continuous_closed_open_range_diff(
            subtract_this=subtract_this,
            from_this=from_this,
        )
    elif range_format == 'named_range':
        assert isinstance(subtract_this, list) and isinstance(from_this, list)
        return [item for item in from_this if item not in set(subtract_this)]
    elif range_format == 'block_range':
        return _get_discrete_closed_range_diff(
            subtract_this=subtract_this, from_this=from_this, discrete_step=1
        )
    elif range_format == 'id_range':
        raise NotImplementedError()
    elif range_format == 'count':
        raise NotImplementedError()
    else:
        raise NotImplementedError()


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
    data_ranges: list[typing.Any], range_format: truck.RangeFormat
) -> list[typing.Any]:
    if range_format == 'date':
        import datetime

        dates = []
        for data_range in data_ranges:
            delta = datetime.timedelta(days=1)
            start_date, end_date = data_range
            current = start_date
            while current <= end_date:
                dates.append(current)
                current = current + delta
        return dates
    else:
        raise NotImplementedError()
