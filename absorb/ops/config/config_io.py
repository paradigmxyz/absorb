from __future__ import annotations

import typing

import absorb
from .. import paths
from . import config_validation

if typing.TYPE_CHECKING:
    import typing_extensions


def get_default_config() -> absorb.Config:
    import os
    import subprocess

    output = subprocess.check_output(['which', 'git'], text=True)
    use_git = os.path.isfile(output.strip())

    return {
        'version': absorb.__version__,
        'tracked_tables': [],
        'use_git': use_git,
        'default_bucket': {
            'provider': None,
            'bucket_name': None,
            'rclone_remote': None,
            'path_prefix': None,
        },
    }


def get_config() -> absorb.Config:
    import json

    try:
        with open(paths.get_config_path(), 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        return get_default_config()

    default_config = get_default_config()
    default_config.update(config)
    config = default_config

    if config_validation.validate_config(config):
        return config
    else:
        raise Exception('invalid config format')


def write_config(
    config: absorb.Config, commit_message: str | None = None
) -> None:
    import json
    import os

    default_config = get_default_config()
    default_config.update(config)
    config = default_config

    if not config_validation.validate_config(config):
        raise Exception('invalid config format')

    path = paths.get_config_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # load old config for change detection
    if os.path.isfile(path):
        with open(path, 'r') as f:
            old_config = json.load(f)
    else:
        old_config = None

    # write config
    with open(path, 'w') as f:
        json.dump(config, f)

    # version control
    if config['use_git']:
        if json.dumps(config, sort_keys=True) != json.dumps(
            old_config, sort_keys=True
        ):
            absorb.ops.git_add_and_commit_file(
                paths.get_config_path(),
                repo_root=paths.get_absorb_root(),
                message=commit_message,
            )
