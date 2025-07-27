from __future__ import annotations

from . import common


class BidTraces(common.XatuTable):
    datatype = 'mev_relay_bid_trace'
    source = 'mev_relay'
    chunk_size = 'hour'


class ProposerPayloadDeliveries(common.XatuTable):
    datatype = 'mev_relay_proposer_payload_delivered'
    source = 'mev_relay'
    chunk_size = 'hour'


class ValidatorRegistrations(common.XatuTable):
    datatype = 'mev_relay_validator_registration'
    source = 'mev_relay'
    chunk_size = 'hour'
