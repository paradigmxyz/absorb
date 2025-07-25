"""python interface for interacting with flashbots mempool dumpster"""

from .types.errors import *
from .types.table_class import Table
from . import ops
from .ops import (
    scan,
    load,
    query,
    sql_query,
    get_available_range,
    get_collected_range,
)

import typing

if typing.TYPE_CHECKING:
    from .types.annotations import *


__version__ = '0.2.0'
