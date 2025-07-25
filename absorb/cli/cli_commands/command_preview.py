from __future__ import annotations

import typing

import absorb
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def preview_command(args: Namespace) -> dict[str, Any]:
    import polars as pl
    import toolstr

    preview_length = args.count
    offset = args.offset

    pl.Config.set_tbl_hide_dataframe_shape(True)
    pl.Config.set_tbl_rows(preview_length)

    datasets = cli_parsing._parse_datasets(args)
    n_rows = {}
    for d, dataset in enumerate(datasets):
        # load dataset preview
        df = (
            absorb.query(dataset)
            .slice(offset)
            .head(preview_length + 1)
            .collect()
        )

        # print number of rows in preview
        if d > 0:
            print()
        if len(datasets) > 1:
            toolstr.print_text_box(dataset.full_name(), style='bold')

        if len(df) > preview_length:
            if offset > 0:
                print(preview_length, 'rows starting from offset', offset)
            else:
                print('first', preview_length, 'rows:')

        # print dataset preview
        print(df.head(preview_length))

        # print total number of rows
        dataset_n_rows = absorb.scan(dataset).select(pl.len()).collect().item()
        n_rows[dataset.full_name()] = dataset_n_rows
        print(dataset_n_rows, 'rows,', len(df.columns), 'columns')

    # load interactive previews
    if args.interactive:
        if len(datasets) == 1:
            dataset = datasets[0]
            if n_rows[dataset.full_name()] <= 1_000_000:
                return {'df': absorb.load(dataset)}
            else:
                return {'lf': absorb.scan(dataset)}
        else:
            dfs = {}
            lfs = {}
            for dataset in datasets:
                table_name = dataset.full_name()
                if n_rows[table_name] <= 1_000_000:
                    dfs[table_name] = absorb.load(dataset)
                else:
                    lfs[table_name] = absorb.scan(dataset)
            outputs: dict[str, typing.Any] = {}
            if len(dfs) > 0:
                outputs['dfs'] = dfs
            if len(lfs) > 0:
                outputs['lfs'] = lfs
            return outputs
    else:
        return {}
