from __future__ import annotations

import argparse
import json
import typing

import rich_argparse

if typing.TYPE_CHECKING:
    Subparsers = argparse._SubParsersAction[argparse.ArgumentParser]


def open_interactive_session(*, variables: dict[str, typing.Any]) -> None:
    print()
    header = (
        'interactive python session started\n\ndata sorted in these variables:'
    )
    for key, value in variables.items():
        header += (
            '\n- \033[1m\033[97m' + key + '\033[0m: ' + type(value).__name__
        )
    try:
        from IPython.terminal.embed import InteractiveShellEmbed

        ipshell = InteractiveShellEmbed(colors='Linux', display_banner=False)  # type: ignore
        print(header)
        ipshell(header=header, local_ns=variables)
    except ImportError:
        import code
        import sys

        class ExitInteract:
            def __call__(self) -> None:
                raise SystemExit

            def __repr__(self) -> str:
                raise SystemExit

        try:
            sys.ps1 = '>>> '
            code.interact(
                banner='\n' + header + '\n',
                local=dict(variables, exit=ExitInteract()),
            )
        except SystemExit:
            pass


def _enter_debugger() -> None:
    """open debugger to most recent exception

    - adapted from https://stackoverflow.com/a/242514
    """
    import sys
    import traceback

    # print stacktrace
    extype, value, tb = sys.exc_info()
    print('[ENTERING DEBUGGER]')

    # print traceback
    try:
        from IPython.core.ultratb import VerboseTB

        vtb = VerboseTB(
            color_scheme='Linux'
        )  # You can also try 'LightBG' or 'Neutral'
        if extype and value and tb:
            formatted_tb = vtb.text(extype, value, tb)
            print(formatted_tb)
        else:
            traceback.print_exc()
    except ImportError:
        traceback.print_exc()
    print()

    # enter debugger
    try:
        import ipdb  # type: ignore
        import types

        tb = typing.cast(types.TracebackType, tb)
        ipdb.post_mortem(tb)

    except ImportError:
        import pdb

        pdb.post_mortem(tb)


class HelpFormatter(rich_argparse.RichHelpFormatter):
    usage_markup = True

    styles = {
        'argparse.prog': 'bold white',
        'argparse.groups': 'bold green',
        'argparse.args': 'bold white',
        'argparse.metavar': 'grey62',
        'argparse.help': 'grey62',
        'argparse.text': 'blue',
        'argparse.syntax': 'blue',
        'argparse.default': 'blue',
    }

    def __init__(self, prog: str) -> None:
        super().__init__(prog, max_help_position=32)

    def _format_args(self, action, default_metavar):  # type: ignore
        get_metavar = self._metavar_formatter(action, default_metavar)
        if action.nargs == argparse.ZERO_OR_MORE:
            return '[%s [%s ...]]' % get_metavar(2)
        elif action.nargs == argparse.ONE_OR_MORE:
            return '%s [...]' % get_metavar(1)
        return super()._format_args(action, default_metavar)

    def format_help(self) -> str:
        lines = [
            line
            for line in super().format_help().split('\n')
            if not line.startswith('  \x1b[1;37m{')
        ]
        return '\n'.join(lines)
