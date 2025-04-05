from __future__ import annotations

import typing

from .. import types
from . import cli_commands
from . import cli_helpers

if typing.TYPE_CHECKING:
    import argparse


def parse_args() -> argparse.Namespace:
    import argparse

    parser = argparse.ArgumentParser(
        formatter_class=cli_helpers.HelpFormatter, allow_abbrev=False
    )
    subparsers = parser.add_subparsers(dest='command')

    parsers = {}
    for name, description, f, arg_args in cli_commands.get_subcommands():
        subparser = subparsers.add_parser(name, help=description)
        subparser.set_defaults(f_command=f)
        for sub_args, sub_kwargs in arg_args:
            subparser.add_argument(*sub_args, **sub_kwargs)
        subparser.add_argument(
            '--debug',
            '--pdb',
            help='enter debugger upon error',
            action='store_true',
        )
        subparser.add_argument(
            '-i',
            '--interactive',
            help='open data in interactive python session',
            action='store_true',
        )
        parsers[name] = subparser

    # parse args
    args = parser.parse_args()

    # display help if no command specified
    if args.command is None:
        import sys

        parser.print_help()
        sys.exit(0)

    return args
