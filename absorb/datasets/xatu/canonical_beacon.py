from __future__ import annotations

from . import common


class BeaconBlocks(common.XatuTable):
    datatype = 'canonical_beacon_block'
    source = 'cannonical_beacon'
    index_type = 'hour'


class BeaconCommittee(common.XatuTable):
    datatype = 'canonical_beacon_committee'
    source = 'cannonical_beacon'
    index_type = 'hour'


class AttesterSlashings(common.XatuTable):
    datatype = 'canonical_beacon_block_attester_slashing'
    source = 'cannonical_beacon'
    index_type = 'hour'


class ProposerSlashings(common.XatuTable):
    datatype = 'canonical_beacon_block_proposer_slashing'
    source = 'cannonical_beacon'
    index_type = 'hour'


class BlockBlsToExecutionChange(common.XatuTable):
    datatype = 'canonical_beacon_block_bls_to_execution_change'
    source = 'cannonical_beacon'
    index_type = 'hour'


class BlockExecutionTransaction(common.XatuTable):
    datatype = 'canonical_beacon_block_execution_transaction'
    source = 'cannonical_beacon'
    index_type = 'hour'


class VoluntaryExits(common.XatuTable):
    datatype = 'canonical_beacon_block_voluntary_exit'
    source = 'cannonical_beacon'
    index_type = 'hour'


class BlockDeposits(common.XatuTable):
    datatype = 'canonical_beacon_block_deposit'
    source = 'cannonical_beacon'
    index_type = 'hour'


class BlockWithdrawals(common.XatuTable):
    datatype = 'canonical_beacon_block_withdrawal'
    source = 'cannonical_beacon'
    index_type = 'hour'


class BlobSidecars(common.XatuTable):
    datatype = 'canonical_beacon_blob_sidecar'
    source = 'cannonical_beacon'
    index_type = 'hour'


class ProposerDuties(common.XatuTable):
    datatype = 'canonical_beacon_proposer_duty'
    source = 'cannonical_beacon'
    index_type = 'hour'


class ElaborateAttestations(common.XatuTable):
    datatype = 'canonical_beacon_elaborated_attestation'
    source = 'cannonical_beacon'
    index_type = 'hour'


class BeaconValidators(common.XatuTable):
    datatype = 'canonical_beacon_validators'
    source = 'cannonical_beacon'
    index_type = 'hour'


class ValidatorsPubkeys(common.XatuTable):
    datatype = 'canonical_beacon_validators_pubkeys'
    source = 'cannonical_beacon'
    index_type = 'hour'
