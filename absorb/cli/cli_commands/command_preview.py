from __future__ import annotations

import typing

import absorb
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def preview_command(args: Namespace) -> dict[str, Any]:
    import polars as pl

    preview_length = args.count
    offset = args.offset

    pl.Config.set_tbl_hide_dataframe_shape(True)
    pl.Config.set_tbl_rows(preview_length)

    datasets = cli_parsing._parse_datasets(args)
    for dataset in datasets:
        df = (
            absorb.scan(dataset)
            .slice(offset)
            .head(preview_length + 1)
            .collect()
        )
        if len(df) > preview_length:
            if offset > 0:
                print(preview_length, 'rows starting from offset', offset)
            else:
                print('first', preview_length, 'rows:')
        print(df.head(preview_length))
        n_rows = absorb.scan(dataset).select(pl.len()).collect().item()
        print(n_rows, 'rows,', len(df.columns), 'columns')

    return {}
