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
        if isinstance(e, SystemExit) and len(e.args) > 0 and e.args[0] == 0:
            pass
        elif args.debug:
            cli_helpers._enter_debugger()
        else:
            import toolstr

            toolstr.print('[red]error:[/red] ' + str(e.args[0]))
