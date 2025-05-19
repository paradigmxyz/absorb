from __future__ import annotations

from . import common


class BeaconBlocks(common.XatuTable):
    datatype = 'canonical_beacon_block'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class BeaconCommittee(common.XatuTable):
    datatype = 'canonical_beacon_committee'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class AttesterSlashings(common.XatuTable):
    datatype = 'canonical_beacon_block_attester_slashing'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class ProposerSlashings(common.XatuTable):
    datatype = 'canonical_beacon_block_proposer_slashing'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class BlockBlsToExecutionChange(common.XatuTable):
    datatype = 'canonical_beacon_block_bls_to_execution_change'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class BlockExecutionTransaction(common.XatuTable):
    datatype = 'canonical_beacon_block_execution_transaction'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class VoluntaryExits(common.XatuTable):
    datatype = 'canonical_beacon_block_voluntary_exit'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class BlockDeposits(common.XatuTable):
    datatype = 'canonical_beacon_block_deposit'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class BlockWithdrawals(common.XatuTable):
    datatype = 'canonical_beacon_block_withdrawal'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class BlobSidecars(common.XatuTable):
    datatype = 'canonical_beacon_blob_sidecar'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class ProposerDuties(common.XatuTable):
    datatype = 'canonical_beacon_proposer_duty'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class ElaborateAttestations(common.XatuTable):
    datatype = 'canonical_beacon_elaborated_attestation'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class BeaconValidators(common.XatuTable):
    datatype = 'canonical_beacon_validators'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'


class ValidatorsPubkeys(common.XatuTable):
    datatype = 'canonical_beacon_validators_pubkeys'
    source = 'cannonical_beacon'
    range_format = 'hour'
    per = 'day'
