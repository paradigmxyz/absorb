"""
file organization style
- defillama__yields_per_pool__uniswap_v2__2025-01-01--01-01-02.000.parquet
- defillama__fees_per_chain__solana__2025-01-01--01-01-02.parquet
^ timestamp in filename is time that file was collected or last timesatmp in file?
"""

from __future__ import annotations

import typing


endpoints = {
    'current_yields': 'https://yields.llama.fi/pools',
    'yields_per_pool': 'https://yields.llama.fi/chart/{pool}',
    'fees': 'https://api.llama.fi/overview/fees',
    'fees_per_chain': 'https://api.llama.fi/overview/fees/{chain}',
    'fees_per_protocol': 'https://api.llama.fi/summary/fees/{protocol}',
}


def get_url_data(url: str) -> typing.Any:
    import requests

    response = requests.get(url)
    return response.json()
