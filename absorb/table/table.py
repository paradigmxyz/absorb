from __future__ import annotations

import typing

from . import table_collect
from . import table_create

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')


class Table(
    table_collect.TableCollect,
    table_create.TableCreate,
):
    pass
