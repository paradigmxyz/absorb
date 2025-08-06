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

    datasets = cli_parsing._parse_datasets(args)
    for dataset in datasets:
        absorb.ops.preview(dataset, n_rows=args.count, offset=args.offset)

    # load interactive previews
    if args.interactive:
        if len(datasets) == 1:
            dataset = datasets[0]
            dataset_n_rows = (
                absorb.ops.scan(dataset).select(pl.len()).collect().item()
            )
            if dataset_n_rows <= 1_000_000:
                return {'df': absorb.ops.load(dataset)}
            else:
                return {'lf': absorb.ops.scan(dataset)}
        else:
            dfs = {}
            lfs = {}
            for dataset in datasets:
                table_name = dataset.full_name()
                dataset_n_rows = (
                    absorb.ops.scan(dataset).select(pl.len()).collect().item()
                )
                if dataset_n_rows <= 1_000_000:
                    dfs[table_name] = absorb.ops.load(dataset)
                else:
                    lfs[table_name] = absorb.ops.scan(dataset)
            outputs: dict[str, typing.Any] = {}
            if len(dfs) > 0:
                outputs['dfs'] = dfs
            if len(lfs) > 0:
                outputs['lfs'] = lfs
            return outputs
    else:
        return {}
