from __future__ import annotations

import typing

import absorb
from . import env

if typing.TYPE_CHECKING:
    import polars as pl


def check_bucket_setup(bucket: absorb.Bucket | None = None) -> str | None:
    # check if rclone package is installed
    if not env.is_package_installed('rclone_python'):
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
        if bucket['rclone_remote'] is None:
            return 'rclone_remote has not been set'
        elif not rclone_python.rclone.check_remote_existing(
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


def fill_bucket_defaults(bucket: absorb.Bucket | None = None) -> absorb.Bucket:
    if bucket is None:
        return get_default_bucket()
    else:
        bucket = bucket.copy()
        for key, value in get_default_bucket().items():
            if bucket.get(key) is None:
                bucket[key] = value  # type: ignore
        return bucket


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


#
# # uploads/downloads
#


def upload(
    table: absorb.TableReference,
    bucket: absorb.Bucket | None = None,
    verbose: bool = True,
) -> None:
    import rclone_python.rclone

    # determine bucket
    if bucket is None:
        bucket = get_default_bucket()

    # check bucket setup
    problem = absorb.ops.check_bucket_setup(bucket=bucket)
    if problem is not None:
        raise Exception(problem)

    # get paths
    table = absorb.Table.instantiate(table)
    table_dir = table.get_table_dir()
    bucket_path = get_rclone_bucket_path(table=table, bucket=bucket)

    # perform upload
    absorb.ops.print_bullet('table', table.full_name())
    absorb.ops.print_bullet('source path', table_dir)
    absorb.ops.print_bullet('destination path', bucket_path)
    print()
    rclone_python.rclone.copy(
        table_dir,
        bucket_path,
        args=['-vv', '--stats', '1s', '--stats-one-line'],
    )


def download(
    table: absorb.TableReference,
    bucket: absorb.Bucket | None = None,
) -> None:
    import rclone_python.rclone

    # determine bucket
    if bucket is None:
        bucket = get_default_bucket()

    # check bucket setup
    problem = absorb.ops.check_bucket_setup(bucket=bucket)
    if problem is not None:
        raise Exception(problem)

    # get paths
    table = absorb.Table.instantiate(table)
    table_dir = table.get_table_dir()
    bucket_path = get_rclone_bucket_path(table=table, bucket=bucket)

    # perform upload
    absorb.ops.print_bullet('table', table.full_name())
    absorb.ops.print_bullet('source path', table_dir)
    absorb.ops.print_bullet('destination path', bucket_path)
    print()
    rclone_python.rclone.copy(bucket_path, table_dir)


#
# # paths
#


def get_raw_bucket_path(
    table: absorb.TableReference,
    bucket: absorb.Bucket | None = None,
) -> str:
    # determine bucket information
    if bucket is None:
        bucket = get_default_bucket()
    bucket_name = bucket['bucket_name']
    if bucket_name is None:
        raise Exception('bucket must be specified')
    path_prefix = bucket['path_prefix']
    if path_prefix is None:
        raise Exception('path_prefix must be specified')

    # determine table
    table = absorb.Table.instantiate(table)

    return (
        bucket_name
        + '/'
        + path_prefix
        + '/datasets/'
        + table.source
        + '/tables/'
        + table.name()
    )


def get_table_bucket_glob(
    table: absorb.TableReference,
    bucket: absorb.Bucket | None = None,
) -> str:
    # get bucket protocol
    bucket = fill_bucket_defaults(bucket)
    if bucket['provider'] == 'gcp':
        protocol = 'gs'
    elif bucket['provider'] == 'aws':
        protocol = 's3'
    else:
        raise Exception()

    raw_path = get_raw_bucket_path(table=table, bucket=bucket)
    return protocol + '://' + raw_path + '/*.parquet'


def get_rclone_bucket_path(
    table: absorb.Table,
    bucket: absorb.Bucket | None = None,
) -> str:
    bucket = fill_bucket_defaults(bucket)
    rclone_remote = bucket.get('rclone_remote', None)
    if rclone_remote is None:
        raise Exception('rclone_remote must be specified')
    raw_bucket_path = get_raw_bucket_path(table=table, bucket=bucket)
    return rclone_remote.strip('/') + ':' + raw_bucket_path
