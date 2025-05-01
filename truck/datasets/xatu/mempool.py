from __future__ import annotations

from . import common


class Transactions(common.XatuTable):
    source = 'mempool'
    per = 'day'
    datatype = 'mempool_transaction'
