from __future__ import annotations

from . import cli_parsing
from . import cli_helpers


def run_cli() -> None:
    args = cli_parsing.parse_args()
    try:
        data = args.f_command(args)
        if args.interactive:
            cli_helpers.open_interactive_session(variables=data)
    except BaseException as e:
        if args.debug:
            cli_helpers._enter_debugger()
        else:
            raise e
