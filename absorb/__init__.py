"""python interface for interacting with flashbots mempool dumpster"""

from .errors import *
from .table import Table
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
    from .annotations import *


__version__ = '0.3.0'
