from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from argparse import Namespace
    from typing import Any


def collect_command(args: Namespace) -> dict[str, Any]:
    print()
    return {}
