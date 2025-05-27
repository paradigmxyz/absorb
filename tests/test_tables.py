import absorb

import pytest


required_attrs = [
    'source',
    'write_range',
]


@pytest.mark.parametrize('table', absorb.ops.get_table_classes())
@pytest.mark.parametrize('attr', required_attrs)
def test_tables_have_attrs(table: type[absorb.Table], attr: str) -> None:
    assert hasattr(table, attr), (
        'missing attribute '
        + attr
        + ' for '
        + str(table.source)
        + '.'
        + str(table.__name__)
    )


@pytest.mark.parametrize('table', absorb.ops.get_table_classes())
def test_tables_implement_get_schema(table: type[absorb.Table]) -> None:
    assert table.get_schema != absorb.Table.get_schema, (
        'missing get_schema() for '
        + str(table.source)
        + '.'
        + str(table.__name__)
    )


@pytest.mark.parametrize('table', absorb.ops.get_table_classes())
def test_tables_implement_collect_chunk(table: type[absorb.Table]) -> None:
    assert table.collect_chunk != absorb.Table.collect_chunk, (
        'missing collect_chunk() for '
        + str(table.source)
        + '.'
        + str(table.__name__)
    )


@pytest.mark.parametrize('table', absorb.ops.get_table_classes())
def test_table_parameter_names_valid(table: type[absorb.Table]) -> None:
    for name in table.parameter_types.keys():
        assert name not in [
            'base_name',
        ]
        assert '__' not in name
        assert not name.startswith('_')
        assert not name.endswith('_')
