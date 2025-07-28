from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


class Commits(absorb.Table):
    source = 'git'
    description = 'Commit history of git repository'
    url = 'https://git-scm.com/'
    write_range = 'overwrite_all'
    index_type = 'id'
    index_column = 'hash'
    parameter_types = {'paths': list[str]}
    require_name = True
    required_packages = ['nitwit >= 1.1']

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'hash': pl.String,
            'author': pl.String,
            'email': pl.String,
            'timestamp': pl.Datetime(time_unit='us', time_zone='UTC'),
            'message': pl.String,
            'parents': pl.String,
            'committer': pl.String,
            'committer_email': pl.String,
            'commit_timestamp': pl.Datetime(time_unit='us', time_zone='UTC'),
            'tree_hash': pl.String,
            'is_merge': pl.Boolean,
            'repo_author': pl.String,
            'repo_name': pl.String,
            'repo_source': pl.String,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import nitwit
        import polars as pl

        dfs = [
            nitwit.collect_commits(path) for path in self.parameters['paths']
        ]
        return pl.concat(dfs)


class Authors(absorb.Table):
    source = 'git'
    description = 'Author stats of git repository'
    url = 'https://git-scm.com/'
    write_range = 'overwrite_all'
    index_type = 'id'
    index_column = 'author'
    parameter_types = {'path': str}
    require_name = True

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'author': pl.String,
            'email': pl.String,
            'first_commit_timestamp': pl.Datetime(
                time_unit='us', time_zone='UTC'
            ),
            'last_commit_timestamp': pl.Datetime(
                time_unit='us', time_zone='UTC'
            ),
            'n_commits': pl.Int64,
            'n_repos': pl.UInt32,
            'repo_source': pl.String,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import nitwit

        dfs = []
        for path in self.parameters['paths']:
            commits = nitwit.collect_commits(self.parameters['path'])
            df = nitwit.collect_authors(commits)
            df = df.with_columns(repo_source=pl.lit(path))
            dfs.append(df)
        return pl.concat(dfs)


class FileDiffs(absorb.Table):
    source = 'git'
    description = 'File diffs of git repository'
    url = 'https://git-scm.com/'
    write_range = 'overwrite_all'
    index_type = 'id'
    index_column = ('hash', 'path')
    parameter_types = {'path': str}
    require_name = True

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'hash': pl.String,
            'insertions': pl.Int64,
            'deletions': pl.Int64,
            'path': pl.String,
            'repo_author': pl.String,
            'repo_name': pl.String,
            'repo_source': pl.String,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import nitwit

        dfs = [
            nitwit.collect_file_diffs(path) for path in self.parameters['paths']
        ]
        return pl.concat(dfs)


class FileDiffStats(absorb.Table):
    source = 'git'
    description = 'File diff statistics of git repository'
    url = 'https://git-scm.com/'
    write_range = 'overwrite_all'
    index_type = 'id'
    index_column = 'hash'
    parameter_types = {'path': str}
    require_name = True

    def get_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        import polars as pl

        return {
            'hash': pl.String,
            'n_changed_files': pl.UInt32,
            'insertions': pl.Int64,
            'deletions': pl.Int64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> absorb.ChunkResult | None:
        import nitwit

        dfs = [
            nitwit.collect_commit_file_diffs(path).with_columns(
                repo_source=pl.lit(path)
            )
            for path in self.parameters['paths']
        ]
        return pl.concat(dfs)
