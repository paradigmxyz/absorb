import absorb
import absorb.catalog.allium
import absorb.catalog.snowflake
import absorb.catalog.dune
import absorb.catalog.bigquery

import pytest


required_attrs = [
    'source',
    'write_range',
    'description',
    'url',
]


general_queries = [
    absorb.catalog.snowflake.Query,
    absorb.catalog.allium.Query,
    absorb.catalog.dune.BaseQuery,
    absorb.catalog.dune.FullQuery,
    absorb.catalog.dune.AppendOnlyQuery,
    absorb.catalog.bigquery.Query,
]


@pytest.mark.parametrize('table', absorb.ops.get_table_classes())
def test_case_switching(table: type[absorb.Table]) -> None:
    name = table.__name__
    as_snake = absorb.ops.names._camel_to_snake(name)
    as_camel = absorb.ops.names._snake_to_camel(as_snake)
    assert name == as_camel


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
    assert getattr(table, attr) is not None, (
        'attribute '
        + attr
        + ' is None for '
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

    # attempt to call get_schema()
    if table in general_queries:
        return
    table_instance = None
    try:
        table_instance = table()
    except Exception:
        pass
    if table_instance is not None:
        table_instance.get_schema()


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
            'class_name',
        ]
        assert '__' not in name
        assert not name.startswith('_')
        assert not name.endswith('_')


@pytest.mark.parametrize('table', absorb.ops.get_table_classes())
def test_overwrite_all_does_not_use_chunks(table: type[absorb.Table]) -> None:
    if table.write_range == 'overwrite_all' and table.chunk_size is not None:
        raise Exception(
            'if write_range is overwrite_all, chunk_size must be None for '
            + str(table.source)
            + '.'
            + str(table.__name__)
        )


@pytest.mark.parametrize('table', absorb.ops.get_table_classes())
def test_can_get_index_type(table: type[absorb.Table]) -> None:
    if table in general_queries:
        return
    try:
        index_type = table.get_index_type()
    except Exception:
        raise Exception(
            'cannot get index type for '
            + str(table.source)
            + '.'
            + str(table.name_classmethod())
        )
    assert index_type is not None, (
        'cannot get index type for '
        + str(table.source)
        + '.'
        + str(table.name_classmethod())
    )


@pytest.mark.parametrize('table', absorb.ops.get_table_classes())
def test_if_has_index_type_get_index_column(table: type[absorb.Table]) -> None:
    if table in general_queries:
        return
    if table.get_index_type() != 'no_index':
        try:
            index_column = table.get_index_column()
        except Exception:
            raise Exception(
                'cannot get index column for '
                + str(table.source)
                + '.'
                + str(table.name_classmethod())
            )
        assert index_column is not None

        try:
            instance = table({})
            schema = instance.get_schema()
            if isinstance(index_column, str):
                assert index_column in schema.keys()
            elif isinstance(index_column, tuple):
                for col in index_column:
                    assert col in schema.keys()
            else:
                raise Exception(
                    'invalid format for index_column in '
                    + instance.full_name()
                    + ': '
                    + str(index_column)
                )
        except Exception as e:
            pass
