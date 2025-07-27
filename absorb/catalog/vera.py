"""
instructions:
1. obtain json manifest from https://export.verifieralliance.org/manifest.json
2. download the files in the manifest
"""

from __future__ import annotations

import functools
import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


class VeraChunkedDataset(absorb.Table):
    source = 'vera'
    vera_filetype: str
    url = 'https://verifieralliance.org/'

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        return {}

    def get_available_range(self) -> absorb.Coverage:
        raise NotImplementedError()
        # return get_current_files(self.vera_filetype)

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        url = 'https://export.verifieralliance.org/' + chunk  # type: ignore
        return absorb.ops.download_parquet_to_dataframe(url=url)


class Code(VeraChunkedDataset):
    description = 'Verifier Alliance code dataset'
    write_range = 'append_only'
    vera_filetype = 'code'
    chunk_size = 100000
    index_column = 'block_number'


class Contracts(VeraChunkedDataset):
    description = 'Verifier Alliance contracts dataset'
    write_range = 'append_only'
    vera_filetype = 'contracts'
    chunk_size = 1000000
    index_column = 'block_number'


class ContractDeployments(VeraChunkedDataset):
    description = 'Verifier Alliance contract deployments dataset'
    write_range = 'append_only'
    vera_filetype = 'contract_deployments'
    chunk_size = 1000000
    index_column = 'block_number'


class CompiledContracts(VeraChunkedDataset):
    description = 'Verifier Alliance compiled contracts dataset'
    write_range = 'append_only'
    vera_filetype = 'compiled_contracts'
    chunk_size = 10000
    index_column = 'block_number'


class CompiledContractsSources(VeraChunkedDataset):
    description = 'Verifier Alliance compiled contract sourcecode dataset'
    write_range = 'append_only'
    vera_filetype = 'compiled_contracts_sources'
    chunk_size = 1000000
    index_column = 'block_number'


class Sources(VeraChunkedDataset):
    description = 'Verifier Alliance sourcecode dataset'
    write_range = 'append_only'
    vera_filetype = 'sources'
    chunk_size = 10000
    index_column = 'block_number'


class VerifiedContracts(VeraChunkedDataset):
    description = 'Verifier Alliance verified contracts dataset'
    write_range = 'append_only'
    vera_filetype = 'verified_contracts'
    chunk_size = 1000000
    index_column = 'block_number'


def get_tables() -> list[type[absorb.Table]]:
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
