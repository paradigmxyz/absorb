from __future__ import annotations

import typing

import absorb
from .. import cli_outputs
from .. import cli_parsing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def sql_command(args: Namespace) -> dict[str, Any]:
    lf = absorb.ops.sql_query(args.sql, backend=args.backend)
    df = lf.collect()
    print(df)

    if args.output_file is not None:
        absorb.ops.write_file(df=df, path=args.output_file)
        print('wrote output to', args.output_file)

    return {}
