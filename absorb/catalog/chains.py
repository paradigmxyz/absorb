from __future__ import annotations

import typing

import requests

import absorb

if typing.TYPE_CHECKING:
    from typing import Mapping, MutableMapping
    import polars as pl


class Chains(absorb.Table):
    source = 'chains'
    description = 'Registry of EVM chains and their chain IDs'
    url = 'https://github.com/ethereum-lists/chains'
    write_range = 'overwrite_all'
    index_type = 'id'
    index_column = 'chain_id'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'name': pl.String,
            'chain_id': pl.Int64,
            'chain_id_hex': pl.String,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import polars as pl

        network_names = get_network_names()
        chain_ids = [int(chain_id) for chain_id in network_names.keys()]
        chain_id_hex = [
            hex(int(chain_id))[2:].rjust(64, '0') for chain_id in chain_ids
        ]
        data = {
            'name': network_names.values(),
            'chain_id': chain_ids,
            'chain_id_hex': chain_id_hex,
        }
        return pl.DataFrame(data, schema=self.get_schema())

    def get_available_range(self) -> absorb.Coverage:
        return (0, len(get_network_data()))


# specialcase the standard name for certain chains
special_cases: Mapping[str, str] = {
    'OP Mainnet': 'optimism',
    'Avalanche C-Chain': 'avalanche',
    'Arbitrum One': 'arbitrum',
    'BNB Smart Chain Mainnet': 'bsc',
    'Genesis Coin': 'genesis_coin',
    'X1 Network': 'x1_network',
    'ThaiChain 2.0 ThaiFi': 'thaifi',
    'WEMIX3.0 Mainnet': 'wemix',
    'WEMIX3.0 Testnet': 'wemix_testnet',
}


def get_network_names() -> Mapping[str, str]:
    # fetch raw network data
    data = get_network_data()
    network_names: MutableMapping[str, str] = {}
    for datum in data:
        # standardize name
        filtered = standardize_name(datum['name'])

        # skip deprecated networks
        if 'deprecated' in filtered:
            continue

        if filtered in network_names.values():
            continue
        network_names[str(datum['chainId'])] = filtered

    return network_names


def get_network_data() -> list[Mapping[str, typing.Any]]:
    url = 'https://chainid.network/chains.json'
    response = requests.get(url)
    return response.json()  # type: ignore


def standardize_name(name: str) -> str:
    """put name into standard format"""

    # special cases
    if name in special_cases:
        return special_cases[name]

    # replace special characters
    name = name.lower()
    name = name.replace(' ', '_')
    name = name.replace('-', '_')
    name = name.replace('___', '_')
    name = name.replace('__', '_')
    name = name.replace('(', '')
    name = name.replace(')', '')

    # remove keywords
    while True:
        remove = [
            '_mainnet',
            '_network',
            '_smart_chain',
            '_l1',
            '_sidechain',
            'sidechain',
            '_chain',
            'chain',
            '_coin',
        ]
        for piece in remove:
            if piece in name:
                name = name.replace(piece, '')
                break
        else:
            break

    # strip stray
    name = name.strip()
    name = name.strip('_')
    name = name.strip('-')

    return name
