from __future__ import annotations

import absorb
from . import cli_parsing
from . import cli_helpers


def run_cli() -> None:
    args = cli_parsing.parse_args()

    if args.absorb_root is not None:
        absorb.ops.set_absorb_root(args.absorb_root)

    try:
        data = args.f_command(args)
        if args.interactive:
            cli_helpers.open_interactive_session(variables=data)
    except BaseException as e:
        if args.debug:
            cli_helpers._enter_debugger()
        else:
            raise e
