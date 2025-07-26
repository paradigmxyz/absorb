"""
file organization style
- defillama__yields_per_pool__uniswap_v2__2025-01-01--01-01-02.000.parquet
- defillama__fees_per_chain__solana__2025-01-01--01-01-02.parquet
^ timestamp in filename is time that file was collected or last timesatmp in file?
"""

from __future__ import annotations

import typing


default_root = 'https://api.llama.fi'
stablecoin_root = 'https://stablecoins.llama.fi'
yield_root = 'https://yields.llama.fi'


endpoints = {
    # tvl
    'current_tvls': default_root + '/protocols',
    'historical_tvl_of_protocol': default_root + '/protocol/{protocol}',
    'historical_tvl': default_root + '/v2/historicalChainTvl',
    'historical_tvl_of_chain': default_root + '/v2/historicalChainTvl/{chain}',
    'current_tvl_of_protocol': default_root + '/tvl/{protocol}',
    'current_tvl_per_chain': default_root + '/v2/chains',
    # stablecoins
    'current_stablecoins': stablecoin_root + '/stablecoins',
    'historical_total_stablecoins': stablecoin_root + '/stablecoincharts/all',
    'historical_stablecoins_of_chain': stablecoin_root
    + '/stablecoincharts/{chain}',
    'historical_stablecoins_of_token': stablecoin_root + '/stablecoin/{token}',
    'current_stablecoins_per_chain': stablecoin_root + '/stablecoinchains',
    'historical_stablecoin_prices': stablecoin_root + '/stablecoinprices',
    # yields
    'current_yields': yield_root + '/pools',
    'historical_yields_per_pool': yield_root + '/chart/{pool}',
    # volumes
    'historical_dex_volume': default_root + '/overview/dexs',
    'historical_dex_volume_of_chain': default_root + '/overview/dexs/{chain}',
    'historical_dex_volume_of_protocol': default_root
    + '/summary/dexs/{protocol}',
    'historical_options_volume': default_root + '/overview/options',
    'historical_options_volume_of_chain': default_root
    + '/overview/options/{chain}',
    'historical_options_volume_of_protocol': default_root
    + '/summary/options/{protocol}',
    # fees and revenue
    'historical_fees': default_root + '/overview/fees',
    'historical_fees_per_chain': default_root + '/overview/fees/{chain}',
    'historical_fees_per_protocol': default_root + '/summary/fees/{protocol}',
}


def _fetch(
    endpoint: str, parameters: dict[str, str] | None = None
) -> typing.Any:
    import requests

    url = _get_url(endpoint, parameters)
    response = requests.get(url, timeout=(5, 60))
    response.raise_for_status()
    return response.json()


def _get_url(
    endpoint: str, parameters: dict[str, str] | None = None
) -> typing.Any:
    url = endpoints[endpoint]
    if parameters is not None:
        url = url.format(**parameters)
    return url
