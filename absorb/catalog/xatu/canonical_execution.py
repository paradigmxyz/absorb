from __future__ import annotations

from . import common


class ExecutionBlocks(common.XatuTable):
    datatype = 'canonical_execution_block'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionTransactions(common.XatuTable):
    datatype = 'canonical_execution_transaction'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionTraces(common.XatuTable):
    datatype = 'canonical_execution_traces'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionLogs(common.XatuTable):
    datatype = 'canonical_execution_logs'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionContracts(common.XatuTable):
    datatype = 'canonical_execution_contracts'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionFourByteCounts(common.XatuTable):
    datatype = 'canonical_execution_four_byte_counts'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionAddressAppearances(common.XatuTable):
    datatype = 'canonical_execution_address_appearances'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionBalanceDiffs(common.XatuTable):
    datatype = 'canonical_execution_balance_diffs'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionBalanceReads(common.XatuTable):
    datatype = 'canonical_execution_balance_reads'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionErc20Transfers(common.XatuTable):
    datatype = 'canonical_execution_erc20_transfers'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionErc721Transfers(common.XatuTable):
    datatype = 'canonical_execution_erc721_transfers'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionNativeTransfers(common.XatuTable):
    datatype = 'canonical_execution_native_transfers'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionNonceDiffs(common.XatuTable):
    datatype = 'canonical_execution_nonce_diffs'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionNonceReads(common.XatuTable):
    datatype = 'canonical_execution_nonce_reads'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionStorageDiffs(common.XatuTable):
    datatype = 'canonical_execution_storage_diffs'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}


class ExecutionStorageReads(common.XatuTable):
    datatype = 'canonical_execution_storage_reads'
    source = 'canonical_execution'
    chunk_size = {'type': 'number_range', 'number_interval': 1000}
