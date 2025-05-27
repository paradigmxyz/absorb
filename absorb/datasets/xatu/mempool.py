from __future__ import annotations

from . import common


class Transactions(common.XatuTable):
    source = 'mempool'
    datatype = 'mempool_transaction'
    index_type = 'hour'
