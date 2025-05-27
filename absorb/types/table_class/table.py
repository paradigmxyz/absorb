from __future__ import annotations

import typing

from . import table_collect
from . import table_coverage

if typing.TYPE_CHECKING:
    T = typing.TypeVar('T')


class Table(
    table_collect.TableCollect,
    table_coverage.TableCoverage,
):
    pass
