"""python interface for interacting with flashbots mempool dumpster"""

from .errors import *
from .table import Table
from . import ops
from .ops import (
    add,
    get_available_range,
    get_collected_range,
    get_collected_tables,
    get_schema,
    preview,
    print_table_info,
    query,
    remove,
    sql_query,
)

import typing

if typing.TYPE_CHECKING:
    from .annotations import *


__version__ = '0.3.1'
