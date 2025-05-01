from __future__ import annotations

from . import common


class BeaconAttestations(common.XatuTable):
    datatype = 'libp2p_gossipsub_beacon_attestation'
    source = 'libp2p'
    per = 'hour'


class BeaconBlocks(common.XatuTable):
    datatype = 'libp2p_gossipsub_beacon_block'
    source = 'libp2p'
    per = 'day'


class BlobSidecars(common.XatuTable):
    datatype = 'libp2p_gossipsub_blob_sidecar'
    source = 'libp2p'
    per = 'day'
