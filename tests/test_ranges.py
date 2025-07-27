from __future__ import annotations

import pytest
import datetime

import absorb


RangeDiffTest = tuple[tuple[str, str], tuple[str, str], list[tuple[str, str]]]


# (from_this, subtract_this, target_output)
closed_date_range_diff_cases = [
    # single interval cases
    # case 1
    (
        ('2025-03-01', '2025-03-01'),
        ('2025-02-03', '2025-02-25'),
        [('2025-03-01', '2025-03-01')],
    ),
    # case 2
    (
        ('2025-03-01', '2025-03-01'),
        ('2025-02-03', '2025-03-01'),
        [],
    ),
    # case 3
    (
        ('2025-03-01', '2025-03-01'),
        ('2025-02-03', '2025-04-01'),
        [],
    ),
    # case 4
    (
        ('2025-03-01', '2025-03-01'),
        ('2025-03-01', '2025-03-01'),
        [],
    ),
    # case 5
    (
        ('2025-03-01', '2025-03-01'),
        ('2025-03-01', '2025-04-01'),
        [],
    ),
    # case 6
    (
        ('2025-03-01', '2025-03-01'),
        ('2025-04-01', '2025-05-01'),
        [('2025-03-01', '2025-03-01')],
    ),
    # large interval cases
    # case 1
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-02-03', '2025-02-25'),
        [('2025-03-01', '2025-04-15')],
    ),
    # case 2
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-02-03', '2025-03-01'),
        [('2025-03-02', '2025-04-15')],
    ),
    # case 3
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-02-03', '2025-03-25'),
        [('2025-03-26', '2025-04-15')],
    ),
    # case 4
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-02-03', '2025-04-15'),
        [],
    ),
    # case 5
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-02-03', '2025-06-25'),
        [],
    ),
    # case 6
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-01', '2025-03-01'),
        [('2025-03-02', '2025-04-15')],
    ),
    # case 7
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-01', '2025-03-25'),
        [('2025-03-26', '2025-04-15')],
    ),
    # case 8
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-01', '2025-04-15'),
        [],
    ),
    # case 9
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-01', '2025-04-25'),
        [],
    ),
    # case 10
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-25', '2025-03-25'),
        [('2025-03-01', '2025-03-24'), ('2025-03-26', '2025-04-15')],
    ),
    # case 11
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-25', '2025-04-02'),
        [('2025-03-01', '2025-03-24'), ('2025-04-03', '2025-04-15')],
    ),
    # case 12
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-25', '2025-04-15'),
        [('2025-03-01', '2025-03-24')],
    ),
    # case 13
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-25', '2025-04-25'),
        [('2025-03-01', '2025-03-24')],
    ),
    # case 14
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-04-15', '2025-04-15'),
        [('2025-03-01', '2025-04-14')],
    ),
    # case 15
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-04-15', '2025-04-25'),
        [('2025-03-01', '2025-04-14')],
    ),
    # case 16
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-05-03', '2025-05-25'),
        [('2025-03-01', '2025-04-15')],
    ),
]


@pytest.mark.parametrize('test', closed_date_range_diff_cases)
def test_closed_date_range_diffs(test: RangeDiffTest) -> None:
    (from_this_str, subtract_this_str, target_output_strs) = test
    from_this = (
        datetime.datetime.strptime(from_this_str[0], '%Y-%m-%d'),
        datetime.datetime.strptime(from_this_str[1], '%Y-%m-%d'),
    )
    subtract_this = (
        datetime.datetime.strptime(subtract_this_str[0], '%Y-%m-%d'),
        datetime.datetime.strptime(subtract_this_str[1], '%Y-%m-%d'),
    )
    target_output = [
        (
            datetime.datetime.strptime(target_output_str[0], '%Y-%m-%d'),
            datetime.datetime.strptime(target_output_str[1], '%Y-%m-%d'),
        )
        for target_output_str in target_output_strs
    ]
    actual_output = absorb.ops.get_range_diff(
        subtract_this=subtract_this,
        from_this=from_this,
        boundary_type='closed',
        chunk_size='day',
    )
    if isinstance(actual_output, tuple):
        actual_output = [actual_output]
    assert target_output == actual_output


# (from_this, subtract_this, target_output)
semiopen_date_range_diff_cases = [
    # case 1
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-02-03', '2025-02-25'),
        [('2025-03-01', '2025-04-15')],
    ),
    # case 2
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-02-03', '2025-03-01'),
        [('2025-03-01', '2025-04-15')],
    ),
    # case 3
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-02-03', '2025-03-25'),
        [('2025-03-25', '2025-04-15')],
    ),
    # case 4
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-02-03', '2025-04-15'),
        [],
    ),
    # case 5
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-02-03', '2025-06-25'),
        [],
    ),
    # case 6
    # skip
    # case 7
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-01', '2025-03-25'),
        [('2025-03-25', '2025-04-15')],
    ),
    # case 8
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-01', '2025-04-15'),
        [],
    ),
    # case 9
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-01', '2025-04-25'),
        [],
    ),
    # case 10
    # skip
    # case 11
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-25', '2025-04-02'),
        [('2025-03-01', '2025-03-25'), ('2025-04-02', '2025-04-15')],
    ),
    # case 12
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-25', '2025-04-15'),
        [('2025-03-01', '2025-03-25')],
    ),
    # case 13
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-03-25', '2025-04-25'),
        [('2025-03-01', '2025-03-25')],
    ),
    # case 14
    # skip
    # case 15
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-04-15', '2025-04-25'),
        [('2025-03-01', '2025-04-15')],
    ),
    # case 16
    (
        ('2025-03-01', '2025-04-15'),
        ('2025-05-03', '2025-05-25'),
        [('2025-03-01', '2025-04-15')],
    ),
]


@pytest.mark.parametrize('test', semiopen_date_range_diff_cases)
def test_semiopen_date_range_diffs(test: RangeDiffTest) -> None:
    (from_this_str, subtract_this_str, target_output_strs) = test
    from_this = (
        datetime.datetime.strptime(from_this_str[0], '%Y-%m-%d'),
        datetime.datetime.strptime(from_this_str[1], '%Y-%m-%d'),
    )
    subtract_this = (
        datetime.datetime.strptime(subtract_this_str[0], '%Y-%m-%d'),
        datetime.datetime.strptime(subtract_this_str[1], '%Y-%m-%d'),
    )
    target_output = [
        (
            datetime.datetime.strptime(target_output_str[0], '%Y-%m-%d'),
            datetime.datetime.strptime(target_output_str[1], '%Y-%m-%d'),
        )
        for target_output_str in target_output_strs
    ]
    actual_output = absorb.ops.get_range_diff(
        subtract_this=subtract_this,
        from_this=from_this,
        boundary_type='semiopen',
        chunk_size='day',
    )
    if isinstance(actual_output, tuple):
        actual_output = [actual_output]
    assert target_output == actual_output
