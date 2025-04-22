"""
file organization style
- defillama__yields_per_pool__uniswap_v2__2025-01-01--01-01-02.000.parquet
- defillama__fees_per_chain__solana__2025-01-01--01-01-02.parquet
^ timestamp in filename is time that file was collected or last timesatmp in file?
"""

from __future__ import annotations

import typing


endpoints = {
    # stablecoins
    'current_stablecoins': 'https://stablecoins.llama.fi/stablecoins',
    'historical_total_stablecoins': 'https://stablecoins.llama.fi/stablecoincharts/all',
    'historical_stablecoins_of_chain': 'https://stablecoins.llama.fi/stablecoincharts/{chain}',
    'historical_stablecoins_of_token': 'https://stablecoins.llama.fi/stablecoin/{token}',
    'current_stablecoins_per_chain': 'https://stablecoins.llama.fi/stablecoinchains',
    'historical_stablecoin_prices': 'https://stablecoins.llama.fi/stablecoinprices',
    # yields
    'current_yields': 'https://yields.llama.fi/pools',
    'yields_per_pool': 'https://yields.llama.fi/chart/{pool}',
    # fees and revenue
    'fees': 'https://api.llama.fi/overview/fees',
    'fees_per_chain': 'https://api.llama.fi/overview/fees/{chain}',
    'fees_per_protocol': 'https://api.llama.fi/summary/fees/{protocol}',
}


def _fetch(
    endpoint: str, parameters: dict[str, str] | None = None
) -> typing.Any:
    import requests

    url = _get_url(endpoint, parameters)
    response = requests.get(url)
    return response.json()


def _get_url(
    endpoint: str, parameters: dict[str, str] | None = None
) -> typing.Any:
    url = endpoints[endpoint]
    if parameters is not None:
        url = url.format(**parameters)
    return url
