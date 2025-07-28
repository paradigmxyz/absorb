from __future__ import annotations

import typing

import absorb


_cache = {'root_dir_warning_shown': False}


def get_absorb_root(*, warn: bool = False) -> str:
    import os

    path = os.environ.get('ABSORB_ROOT')
    if path is None or path == '':
        if warn and not _cache['root_dir_warning_shown']:
            import rich

            rich.print(
                '[#777777]using default value for ABSORB_ROOT: ~/absorb\n(set a value for the ABSORB_ROOT env var to remove this message)[/#777777]'
            )
            _cache['root_dir_warning_shown'] = True
        path = '~/absorb'
    path = os.path.expanduser(path)
    return path


def set_absorb_root(path: str) -> None:
    import os

    os.environ['ABSORB_ROOT'] = path
    _cache['root_dir_warning_shown'] = False


def get_config_path(*, warn: bool = False) -> str:
    import os

    return os.path.join(
        absorb.ops.get_absorb_root(warn=warn), 'absorb_config.json'
    )


def get_datasets_dir(*, warn: bool = False) -> str:
    import os

    return os.path.join(absorb.ops.get_absorb_root(warn=warn), 'datasets')


def get_source_tables_dir(source: str, *, warn: bool = False) -> str:
    import os

    return os.path.join(get_datasets_dir(warn=warn), source, 'tables')


def get_source_dir(source: str, *, warn: bool = False) -> str:
    import os

    return os.path.join(get_datasets_dir(warn=warn), source)
