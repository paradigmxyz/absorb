from __future__ import annotations

from . import common


class BeaconComitteeEvents(common.XatuTable):
    datatype = 'beacon_api_eth_v1_beacon_committee'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconAttestationEvents(common.XatuTable):
    datatype = 'beacon_api_eth_v1_events_attestation'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconBlobSidecarEvents(common.XatuTable):
    datatype = 'beacon_api_eth_v1_events_blob_sidecar'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconBlockEventsV1(common.XatuTable):
    datatype = 'beacon_api_eth_v1_events_block'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconChainReorgEvents(common.XatuTable):
    datatype = 'beacon_api_eth_v1_events_chain_reorg'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconContributionAndProofEvents(common.XatuTable):
    datatype = 'beacon_api_eth_v1_events_contribution_and_proof'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconFinalizedCheckpointEvents(common.XatuTable):
    datatype = 'beacon_api_eth_v1_events_finalized_checkpoint'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconHeadEvents(common.XatuTable):
    datatype = 'beacon_api_eth_v1_events_head'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconVoluntaryExitEvents(common.XatuTable):
    datatype = 'beacon_api_eth_v1_events_voluntary_exit'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconValidatorAttestationEvents(common.XatuTable):
    datatype = 'beacon_api_eth_v1_validator_attestation_data'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconBlockEventsV2(common.XatuTable):
    datatype = 'beacon_api_eth_v2_beacon_block'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconProposerDutyEvents(common.XatuTable):
    datatype = 'beacon_api_eth_v1_proposer_duty'
    source = 'beacon_api'
    chunk_size = 'hour'


class BeaconValidatorEvents(common.XatuTable):
    datatype = 'beacon_api_eth_v3_validator_block'
    source = 'beacon_api'
    chunk_size = 'hour'
