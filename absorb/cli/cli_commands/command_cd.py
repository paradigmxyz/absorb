from __future__ import annotations

import typing

import absorb
from .. import cli_parsing
from . import command_path

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


cd_snippet_template = """function absorb {
    local tempfile="$(mktemp -t tmp.XXXXXX)"
    command absorb "$@" --cd-destination-tempfile "$tempfile"
    if [[ -s "$tempfile" ]]; then
        cd "$(realpath $(cat "$tempfile"))"
    fi
    rm -f "$tempfile" 2>/dev/null
}

function abs() {
    absorb "$@"
}
"""


def cd_command(args: Namespace) -> dict[str, Any]:
    # get path
    path = command_path._get_path(args)

    if args.cd_destination_tempfile is None:
        import sys

        print('using the cd subcommand requires special configuration')
        print()
        print(
            'add the following snippet to your shell config (e.g. ~/.profile):'
        )
        print()
        print(cd_snippet_template)
        sys.exit(0)

    # change pwd to path
    with open(args.cd_destination_tempfile, 'w') as f:
        f.write(path)

    return {}
