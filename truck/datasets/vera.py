"""
instructions:
1. obtain json manifest from https://export.verifieralliance.org/manifest.json
2. download the files in the manifest
"""

from __future__ import annotations

import functools
import typing

import truck

if typing.TYPE_CHECKING:
    import polars as pl


class VeraChunkedDataset(truck.Table):
    vera_filetype: str
    range_format = 'block_range'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.Datatype]]:
        raise NotImplementedError()

    def get_available_range(self) -> typing.Any:
        return get_current_files(self.vera_filetype)

    def collect_chunk(self, data_range: typing.Any) -> pl.DataFrame | None:
        url = 'https://export.verifieralliance.org/' + data_range
        return truck.ops.download_parquet_to_dataframe(url=url)


class Code(VeraChunkedDataset):
    source = 'vera'
    write_range = 'overwrite_all'
    vera_filetype = 'code'


class Contracts(VeraChunkedDataset):
    source = 'vera'
    write_range = 'overwrite_all'
    vera_filetype = 'contracts'


class ContractDeployments(VeraChunkedDataset):
    source = 'vera'
    write_range = 'overwrite_all'
    vera_filetype = 'contract_deployments'


class CompiledContracts(VeraChunkedDataset):
    source = 'vera'
    write_range = 'overwrite_all'
    vera_filetype = 'compiled_contracts'


class CompiledContractsSources(VeraChunkedDataset):
    source = 'vera'
    write_range = 'overwrite_all'
    vera_filetype = 'compiled_contracts_sources'


class Sources(VeraChunkedDataset):
    source = 'vera'
    write_range = 'overwrite_all'
    vera_filetype = 'sources'


class VerifiedContracts(VeraChunkedDataset):
    source = 'vera'
    write_range = 'overwrite_all'
    vera_filetype = 'verified_contracts'


def get_tables() -> list[type[truck.Table]]:
    return [
        Code,
        Contracts,
        ContractDeployments,
        CompiledContracts,
        CompiledContractsSources,
        Sources,
        VerifiedContracts,
    ]


@functools.lru_cache
def get_current_manifest() -> dict[str, typing.Any]:
    import requests

    url = 'https://export.verifieralliance.org/manifest.json'
    response = requests.get(url)
    response.raise_for_status()
    manifest: dict[str, typing.Any] = response.json()
    return manifest


def get_current_files(filetype: str) -> list[str]:
    manifest = get_current_manifest()
    if filetype in [
        'code',
        'contracts',
        'contract_deployments',
        'compiled_contracts',
        'compiled_contracts_sources',
        'sources',
        'verified_contracts',
    ]:
        files = manifest['files'][filetype]
        if not isinstance(files, list) or not all(
            isinstance(item, str) for item in files
        ):
            raise Exception()
        return files
    else:
        raise Exception('invalid filetype')
