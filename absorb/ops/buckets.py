from __future__ import annotations

import typing

import absorb
from . import config

if typing.TYPE_CHECKING:
    import polars as pl


def check_bucket_setup(bucket: absorb.Bucket | None = None) -> str | None:
    # check if rclone package is installed
    if not config.is_package_installed('rclone_python'):
        return 'rclone_python is not installed. Install it before using buckets (for example, `uv add rclone_python`)'

    # check if rclone is installed
    import rclone_python.rclone  # type: ignore

    if not rclone_python.rclone.is_installed():
        return 'rclone is not installed. Install it before using buckets (for example, `brew install rclone`)'

    # check that remote is setup
    remotes = rclone_python.rclone.get_remotes()
    if len(remotes) == 0:
        return 'No rclone remotes are configured. Configure a remote using `rclone config` on the command line'

    if bucket is not None:
        for key in ['rclone_remote', 'bucket_name', 'path_prefix']:
            if key not in bucket:
                return f'Bucket configuration is missing required key: {key}'
        if not rclone_python.rclone.check_remote_existing(
            bucket['rclone_remote']
        ):
            return (
                'rclone remote '
                + str(bucket['rclone_remote'])
                + ' does not exist. Check your rclone configuration.'
            )

    return None


def get_default_bucket() -> absorb.Bucket:
    return absorb.ops.get_config()['default_bucket']


#
# # bucket scanning
#


def scan_bucket(
    table: absorb.TableReference,
    bucket: absorb.Bucket | None = None,
    scan_kwargs: dict[str, typing.Any] | None = None,
    verbose: bool = True,
) -> pl.LazyFrame:
    import polars as pl

    glob = get_table_bucket_glob(bucket=bucket, table=table)
    if scan_kwargs is None:
        scan_kwargs = {}
    if verbose:
        print('scanning remote bucket:', glob)
    return pl.scan_parquet(glob, **scan_kwargs)


def get_table_bucket_glob(
    table: absorb.TableReference,
    bucket: absorb.Bucket | None = None,
) -> str:
    # determine bucket
    if bucket is None:
        bucket = get_default_bucket()

    # get bucket protocol
    if bucket['provider'] == 'gcp':
        protocol = 'gs'
    elif bucket['provider'] == 'aws':
        protocol = 's3'
    else:
        raise Exception()
    bucket_name = bucket['bucket_name']
    if bucket_name is None:
        raise Exception('bucket must be specified')
    path_prefix = bucket.get('path_prefix')
    if path_prefix is None:
        raise Exception('path_prefix must be specified')

    # resolve table
    table = absorb.Table.instantiate(table)

    return (
        protocol
        + '://'
        + bucket_name
        + '/'
        + path_prefix
        + '/datasets/'
        + table.source
        + '/tables/'
        + table.name()
        + '/*.parquet'
    )


#
# # uploads/downloads
#


def upload_tables_to_bucket(
    tables: typing.Sequence[absorb.TableReference],
    bucket: absorb.Bucket | None = None,
) -> None:
    # determine bucket
    if bucket is None:
        bucket = get_default_bucket()

    # check bucket setup
    problem = absorb.ops.check_bucket_setup(bucket=bucket)
    if problem is not None:
        raise Exception(problem)

    # upload each bucket
    for table in tables:
        _upload_table_to_bucket(table, bucket)


def download_tables_to_bucket(
    tables: typing.Sequence[absorb.TableReference],
    bucket: absorb.Bucket | None = None,
) -> None:
    # determine bucket
    if bucket is None:
        bucket = get_default_bucket()

    # check bucket setup
    problem = absorb.ops.check_bucket_setup(bucket=bucket)
    if problem is not None:
        raise Exception(problem)

    # upload each bucket
    for table in tables:
        _download_table_from_bucket(table, bucket)


def _upload_table_to_bucket(
    table: absorb.TableReference,
    bucket: absorb.Bucket,
) -> None:
    import rclone_python.rclone

    # get paths
    table = absorb.Table.instantiate(table)
    table_dir = table.get_table_dir()
    bucket_path = get_rclone_bucket_path(table=table, **bucket)

    print('uploading', table_dir, 'to', bucket_path)

    # perform upload
    rclone_python.rclone.copy(table_dir, bucket_path)


def _download_table_from_bucket(
    table: absorb.TableReference,
    bucket: absorb.Bucket,
) -> None:
    import rclone_python.rclone

    # get paths
    table = absorb.Table.instantiate(table)
    table_dir = table.get_table_dir()
    bucket_path = get_rclone_bucket_path(table=table, **bucket)

    # perform upload
    rclone_python.rclone.copy(bucket_path, table_dir)


def get_rclone_bucket_path(
    *,
    rclone_remote: str | None,
    bucket_name: str | None,
    path_prefix: str | None,
    table: absorb.Table,
    provider: str | None = None,
) -> str:
    if rclone_remote is None:
        raise Exception('rclone_remote must be specified')
    if bucket_name is None:
        raise Exception('bucket_name must be specified')
    if path_prefix is None:
        raise Exception('path_prefix must be specified')
    return (
        rclone_remote.strip('/')
        + ':'
        + bucket_name.strip('/')
        + '/'
        + path_prefix.strip('/')
        + '/datasets/'
        + table.source
        + '/tables/'
        + table.name()
    )
