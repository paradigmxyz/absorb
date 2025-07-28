# https://docs.growthepie.xyz/api

from __future__ import annotations

import absorb

import typing

if typing.TYPE_CHECKING:
    import polars as pl


root = 'https://l2beat.com/api/'

endpoints = {
    'summary': root + 'scaling/summary?range=max',
    'value': root + 'scaling/tvs?range=max',
    'project_tvs': root + 'scaling/tvs/{project}?range=max',
    'activity': root + 'scaling/activity?range=max',
    'project_activity': root + 'scaling/activity/{project}?range=max',
}


class Metrics(absorb.Table):
    source = 'l2beat'
    description = 'On-chain metrics for Ethereum and its rollups'
    url = 'https://l2beat.com/'
    write_range = 'overwrite_all'
    row_precision = 'day'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'timestamp': pl.Datetime('us', 'UTC'),
            'n_transactions': pl.Int64,
            'n_user_operations': pl.Int64,
            'native_tvs': pl.Float64,
            'canonical_tvs': pl.Float64,
            'external_tvs': pl.Float64,
            'total_tvs': pl.Float64,
            'chain': pl.String,
            'layer': pl.String,
            'category': pl.String,
            'da': pl.String,
            'stack': pl.String,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        return get_all_data()

    def get_available_range(self) -> absorb.Coverage:
        import datetime
        import requests

        first = datetime.datetime(year=2019, month=11, day=15)
        response = requests.get(endpoints['value'])
        data = response.json()
        last_timestamp = data['data']['chart']['data'][-1][0]
        last = datetime.datetime.fromtimestamp(last_timestamp)
        return (first, last)


def get_projects() -> pl.DataFrame:
    import requests
    import polars as pl

    response = requests.get(endpoints['summary'])
    data = response.json()
    return pl.DataFrame(list(data['projects'].values()), orient='row')


def get_project_activity(project: str) -> pl.DataFrame:
    import requests
    import polars as pl

    response = requests.get(
        endpoints['project_activity'].format(project=project)
    )
    data = response.json()
    if not data['success']:
        raise Exception(data['error'])
    return (
        pl.DataFrame(
            data['data']['chart']['data'],
            schema=data['data']['chart']['types'],
            orient='row',
        )
        .with_columns(
            (pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC'))
        )
        .rename({'count': 'n_transactions', 'uopsCount': 'n_user_operations'})
    )


def get_project_tvs(project: str) -> pl.DataFrame:
    import requests
    import polars as pl

    response = requests.get(endpoints['project_tvs'].format(project=project))
    data = response.json()
    if not data['success']:
        raise Exception(data['error'])
    return pl.DataFrame(
        data['data']['chart']['data'],
        schema=data['data']['chart']['types'],
        orient='row',
    ).select(
        timestamp=(pl.col.timestamp * 1000000).cast(pl.Datetime('us', 'UTC')),
        native_tvs=pl.col.native.cast(pl.Float64),
        canonical_tvs=pl.col.canonical.cast(pl.Float64),
        external_tvs=pl.col.external.cast(pl.Float64),
        total_tvs=(pl.col.native + pl.col.canonical + pl.col.external).cast(
            pl.Float64
        ),
    )


def get_all_data(*, projects: pl.DataFrame | None = None) -> pl.DataFrame:
    import polars as pl
    import time

    if projects is None:
        projects = get_projects()

    dfs = []
    for project in projects.to_dicts():
        print('getting', project['slug'])
        try:
            time.sleep(20)
            activity = get_project_activity(project['slug'])
            time.sleep(20)
            tvs = get_project_tvs(project['slug'])
        except Exception as e:
            print('skipping ' + project['slug'] + ' because ' + str(e.args[0]))
            continue
        providers = project.get('providers')
        if providers is None:
            providers = []
        provider = ', '.join(providers)
        df = (
            activity.join(tvs, on='timestamp', how='full', coalesce=True)
            .with_columns(
                chain=pl.lit(project['name']),
                layer=pl.lit(project['type']),
                category=pl.lit(project['category']),
                da=pl.lit(project['hostChain']),
                stack=pl.lit(provider),
            )
            .sort('timestamp')
        )
        dfs.append(df)

    return pl.concat(dfs)
