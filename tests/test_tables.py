import truck

import pytest


@pytest.mark.parametrize('table', truck.ops.get_table_classes())
def test_tables_have_attrs(table: type[truck.Table]) -> None:
    for attr in [
        'source',
        'write_range',
    ]:
        assert hasattr(table, attr), (
            'missing attribute '
            + attr
            + ' for '
            + str(table.source)
            + '.'
            + str(table.__name__)
        )


@pytest.mark.parametrize('table', truck.ops.get_table_classes())
def test_tables_implement_get_schema(table: type[truck.Table]) -> None:
    assert table.get_schema != truck.Table.get_schema, (
        'missing get_schema() for '
        + str(table.source)
        + '.'
        + str(table.__name__)
    )


@pytest.mark.parametrize('table', truck.ops.get_table_classes())
def test_tables_implement_collect_chunk(table: type[truck.Table]) -> None:
    assert table.collect_chunk != truck.Table.collect_chunk, (
        'missing collect_chunk() for '
        + str(table.source)
        + '.'
        + str(table.__name__)
    )


@pytest.mark.parametrize('table', truck.ops.get_table_classes())
def test_table_parameter_names_valid(table: type[truck.Table]) -> None:
    for name in table.parameter_types.keys():
        assert name not in [
            'base_name',
        ]
        assert '__' not in name
        assert not name.startswith('_')
        assert not name.endswith('_')
