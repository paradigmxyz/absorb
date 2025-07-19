from __future__ import annotations

import typing

import absorb
from .. import cli_outputs

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def validate_command(args: Namespace) -> dict[str, Any]:
    import sys
    import os
    import toolstr

    errors = []

    # check that each dataset has a table_metadata.json
    datasets_dir = absorb.ops.get_datasets_dir()
    if not os.path.isdir(datasets_dir):
        print('no datasets collected')
        sys.exit(0)
    for source in os.listdir(datasets_dir):
        source_dir = os.path.join(datasets_dir, source)
        tables_dir = os.path.join(source_dir, 'tables')
        for table in os.listdir(tables_dir):
            metadata_path = os.path.join(
                tables_dir, table, 'table_metadata.json'
            )
            if not os.path.isfile(metadata_path):
                errors.append(
                    f'{source}/{table} is missing table_metadata.json'
                )

    # print errors
    if len(errors) > 0:
        toolstr.print_text_box('Errors Found', style='red')
        for e, error in enumerate(errors):
            toolstr.print_bullet('[red]' + error + '[/red]', number=e + 1)
    else:
        toolstr.print('no errors found', style='green')

    return {}
