from __future__ import annotations

import typing

import absorb

if typing.TYPE_CHECKING:
    import polars as pl


class Commits(absorb.Table):
    source = 'git'
    write_range = 'overwrite_all'
    parameter_types = {'paths': list[str]}
    index_type = 'name'
    require_name = True
    required_packages = ['nitwit >= 1.1']

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'hash': pl.String,
            'author': pl.String,
            'email': pl.String,
            'timestamp': pl.Datetime(time_unit='ms', time_zone='UTC'),
            'message': pl.String,
            'parents': pl.String,
            'committer': pl.String,
            'committer_email': pl.String,
            'commit_timestamp': pl.Datetime(time_unit='ms', time_zone='UTC'),
            'tree_hash': pl.String,
            'is_merge': pl.Boolean,
            'repo_author': pl.String,
            'repo_name': pl.String,
            'repo_source': pl.String,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> pl.DataFrame:
        import nitwit
        import polars as pl

        dfs = [
            nitwit.collect_commits(path) for path in self.parameters['paths']
        ]
        return pl.concat(dfs)


class Authors(absorb.Table):
    source = 'git'
    write_range = 'overwrite_all'
    parameter_types = {'path': str}
    require_name = True
    index_type = 'name'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'author': pl.String,
            'email': pl.String,
            'first_commit_timestamp': pl.Datetime(
                time_unit='ms', time_zone='UTC'
            ),
            'last_commit_timestamp': pl.Datetime(
                time_unit='ms', time_zone='UTC'
            ),
            'n_commits': pl.Int64,
            'n_repos': pl.UInt32,
            'repo_source': pl.String,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> pl.DataFrame:
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
    write_range = 'overwrite_all'
    parameter_types = {'path': str}
    require_name = True
    index_type = 'name'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
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

    def collect_chunk(self, chunk: absorb.Chunk) -> pl.DataFrame:
        import nitwit

        dfs = [
            nitwit.collect_file_diffs(path) for path in self.parameters['paths']
        ]
        return pl.concat(dfs)


class FileDiffStats(absorb.Table):
    source = 'git'
    write_range = 'overwrite_all'
    parameter_types = {'path': str}
    require_name = True
    index_type = 'name'

    def get_schema(self) -> dict[str, type[pl.DataType] | pl.DataType]:
        import polars as pl

        return {
            'hash': pl.String,
            'n_changed_files': pl.UInt32,
            'insertions': pl.Int64,
            'deletions': pl.Int64,
        }

    def collect_chunk(self, chunk: absorb.Chunk) -> pl.DataFrame:
        import nitwit

        dfs = [
            nitwit.collect_commit_file_diffs(path).with_columns(
                repo_source=pl.lit(path)
            )
            for path in self.parameters['paths']
        ]
        return pl.concat(dfs)
