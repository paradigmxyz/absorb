from __future__ import annotations

import truck


_cache = {'root_dir_warning_shown': False}


def get_truck_root(*, warn: bool = False) -> str:
    import os

    path = os.environ.get('TRUCK_ROOT')
    if path is None or path == '':
        if warn and not _cache['root_dir_warning_shown']:
            import rich

            rich.print(
                '[#777777]using default value for TRUCK_ROOT: ~/truck[/#777777]\n'
            )
            _cache['root_dir_warning_shown'] = True
        path = '~/truck'
    path = os.path.expanduser(path)
    return path


def get_config_path(*, warn: bool = False) -> str:
    import os

    return os.path.join(truck.get_truck_root(warn=warn), 'truck_config.json')


def get_source_dir(source: str, *, warn: bool = False) -> str:
    import os

    return os.path.join(get_truck_root(warn=warn), 'datasets', source)


def get_table_dir(
    dataset: truck.TrackedTable | str | None = None,
    *,
    source: str | None = None,
    table: str | None = None,
    warn: bool = False,
) -> str:
    import os

    if isinstance(dataset, str):
        source, table = dataset.split('.')
    elif isinstance(dataset, dict):
        source = dataset['source_name']
        table = dataset['table_name']
    elif source is not None and table is not None:
        pass
    else:
        raise Exception('invalid format')

    source_dir = get_source_dir(source, warn=warn)
    return os.path.join(source_dir, 'tables', table)


def get_table_glob(
    dataset: truck.TrackedTable | str, *, warn: bool = False
) -> str:
    import os

    return os.path.join(get_table_dir(dataset, warn=warn), '*.parquet')


def get_table_metadata_path(
    dataset: truck.TrackedTable | str, *, warn: bool = False
) -> str:
    import os

    return os.path.join(
        truck.get_table_dir(dataset, warn=warn), 'table_metadata.json'
    )


#
# # old
#

# def get_filename(
#     year: int | str | None = None,
#     month: int | str | None = None,
#     day: int | str | None = None,
#     timestamp: tooltime.Timestamp | None = None,
# ) -> str:
#     import tooltime

#     if timestamp is not None:
#         dt = tooltime.timestamp_to_datetime(timestamp)
#         return '{year:04}-{month:02}-{day:02}.parquet'.format(
#             year=dt.year,
#             month=dt.month,
#             day=dt.day,
#         )
#     else:
#         if year is None:
#             year = '*'
#         elif isinstance(year, int):
#             year = '%04d' % year
#         if month is None:
#             month = '*'
#         elif isinstance(month, int):
#             month = '%02d' % month
#         if day is None:
#             day = '*'
#         elif isinstance(day, int):
#             day = '%02d' % day
#         return '{year}-{month}-{day}.parquet'.format(
#             year=year, month=month, day=day
#         )


# def get_path(
#     year: int | str | None = None,
#     month: int | str | None = None,
#     day: int | str | None = None,
#     timestamp: tooltime.Timestamp | None = None,
#     flat: bool | None = None,
#     root_dir: str | None = None,
# ) -> str:
#     import os

#     dirpath = get_dir(
#         year=year,
#         month=month,
#         day=day,
#         timestamp=timestamp,
#         flat=flat,
#         root_dir=root_dir,
#     )
#     filename = get_filename(
#         year=year,
#         month=month,
#         day=day,
#         timestamp=timestamp,
#     )
#     return os.path.join(dirpath, filename)


# def get_paths(
#     start_time: tooltime.Timestamp,
#     end_time: tooltime.Timestamp,
#     root_dir: str | None = None,
#     flat: bool | None = None,
# ) -> list[str]:
#     import tooltime

#     timestamps = tooltime.get_intervals(
#         start=start_time,
#         end=end_time,
#         interval='1d',
#     )
#     return [
#         get_path(
#             timestamp=timestamp,
#             flat=flat,
#             root_dir=root_dir,
#         )
#         for timestamp in timestamps['start']
#     ]


# def get_dir(
#     *,
#     year: int | str | None = None,
#     month: int | str | None = None,
#     day: int | str | None = None,
#     timestamp: tooltime.Timestamp | None = None,
#     flat: bool | None = None,
#     root_dir: str | None = None,
# ) -> str:
#     import os
#     import tooltime

#     # get root dir
#     if root_dir is None:
#         root_dir = get_truck_root()
#     dirpath = root_dir

#     # add year-month subdir
#     if flat is None:
#         dir_files = os.listdir(root_dir)
#         if any(file.endswith('.parquet') for file in dir_files):
#             flat = True
#         elif len(dir_files) > 1:
#             flat = False
#         else:
#             flat = True
#     if not flat:
#         if timestamp is not None:
#             dt = tooltime.timestamp_to_datetime(timestamp)
#             year = dt.year
#             month = dt.month
#         elif year is not None and month is not None:
#             pass
#         else:
#             raise Exception('need more date information for non-flat path')
#         year_month = str(year) + '-' + ('%02d' % dt.month)
#         dirpath = os.path.join(dirpath, year_month)

#     return dirpath
