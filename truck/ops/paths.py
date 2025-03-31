
_cache = {'root_dir_warning_shown': False}

def get_root_dir() -> str:
    import os

    root_dir = os.environ.get('MEMPOOL_DUMPSTER_ROOT')
    if root_dir is not None:
        return root_dir
    else:
        if not _cache['root_dir_warning_shown']:
            print(
                'using default value for MEMPOOL_DUMPSTER_ROOT: ~/mempool_dumpster'
            )
            _cache['root_dir_warning_shown'] = True
        return os.path.expanduser('~/mempool_dumpster')

def get_filename(
    year: int | str | None = None,
    month: int | str | None = None,
    day: int | str | None = None,
    timestamp: tooltime.Timestamp | None = None,
) -> str:
    import tooltime

    if timestamp is not None:
        dt = tooltime.timestamp_to_datetime(timestamp)
        return '{year:04}-{month:02}-{day:02}.parquet'.format(
            year=dt.year,
            month=dt.month,
            day=dt.day,
        )
    else:
        if year is None:
            year = '*'
        elif isinstance(year, int):
            year = '%04d' % year
        if month is None:
            month = '*'
        elif isinstance(month, int):
            month = '%02d' % month
        if day is None:
            day = '*'
        elif isinstance(day, int):
            day = '%02d' % day
        return '{year}-{month}-{day}.parquet'.format(
            year=year, month=month, day=day
        )

def get_path(
    year: int | str | None = None,
    month: int | str | None = None,
    day: int | str | None = None,
    timestamp: tooltime.Timestamp | None = None,
    flat: bool | None = None,
    root_dir: str | None = None,
) -> str:
    import os

    dirpath = get_dir(
        year=year,
        month=month,
        day=day,
        timestamp=timestamp,
        flat=flat,
        root_dir=root_dir,
    )
    filename = get_filename(
        year=year,
        month=month,
        day=day,
        timestamp=timestamp,
    )
    return os.path.join(dirpath, filename)

def get_paths(
    start_time: tooltime.Timestamp,
    end_time: tooltime.Timestamp,
    root_dir: str | None = None,
    flat: bool | None = None,
) -> list[str]:
    import tooltime

    timestamps = tooltime.get_intervals(
        start=start_time,
        end=end_time,
        interval='1d',
    )
    return [
        get_path(
            timestamp=timestamp,
            flat=flat,
            root_dir=root_dir,
        )
        for timestamp in timestamps['start']
    ]

def get_dir(
    *,
    year: int | str | None = None,
    month: int | str | None = None,
    day: int | str | None = None,
    timestamp: tooltime.Timestamp | None = None,
    flat: bool | None = None,
    root_dir: str | None = None,
) -> str:
    import os
    import tooltime

    # get root dir
    if root_dir is None:
        root_dir = get_root_dir()
    dirpath = root_dir

    # add year-month subdir
    if flat is None:
        dir_files = os.listdir(root_dir)
        if any(file.endswith('.parquet') for file in dir_files):
            flat = True
        elif len(dir_files) > 1:
            flat = False
        else:
            flat = True
    if not flat:
        if timestamp is not None:
            dt = tooltime.timestamp_to_datetime(timestamp)
            year = dt.year
            month = dt.month
        elif year is not None and month is not None:
            pass
        else:
            raise Exception('need more date information for non-flat path')
        year_month = str(year) + '-' + ('%02d' % dt.month)
        dirpath = os.path.join(dirpath, year_month)

    return dirpath
