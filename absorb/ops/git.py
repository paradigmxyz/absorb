from __future__ import annotations

import absorb


def setup_git(track_tables: list[absorb.Table] | None = None) -> None:
    import os

    absorb_root = absorb.ops.get_absorb_root()

    # initialize repo
    if not git_is_repo_root(absorb_root):
        git_initialize_repo(absorb_root)

    # setup gitignore
    gitignore_path = os.path.join(absorb_root, '.gitignore')
    if not os.path.isfile(gitignore_path):
        default_gitignore = '*.parquet'
        with open(gitignore_path, 'w') as f:
            f.write(default_gitignore)
    if not git_is_file_tracked(gitignore_path, repo_root=absorb_root):
        git_add_and_commit_file(gitignore_path, repo_root=absorb_root)

    # add config file
    config_path = absorb.ops.get_config_path()
    if not git_is_file_tracked(config_path, repo_root=absorb_root):
        git_add_and_commit_file(config_path, repo_root=absorb_root)

    # add metadata of existing tables
    if track_tables is not None:
        n_added = 0
        for table in track_tables:
            metadata_path = table.get_table_metadata_path()
            if not git_is_file_tracked(metadata_path, repo_root=absorb_root):
                git_add_file(metadata_path, repo_root=absorb_root)
                n_added += 1
        if n_added > 0:
            git_commit(
                message='added ' + str(n_added) + ' table metadata files',
                repo_root=absorb_root,
            )


def git_is_in_repo(path: str) -> bool:
    """Check if a directory is inside a git repository"""
    import subprocess
    import os

    # Ensure the path exists
    if not os.path.exists(path):
        return False

    # If it's a file, use its directory
    if os.path.isfile(path):
        path = os.path.dirname(path)

    try:
        subprocess.run(
            ['git', 'rev-parse', '--git-dir'],
            cwd=path,
            check=True,
            capture_output=True,
            text=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False


def git_is_file_tracked(path: str, repo_root: str) -> bool:
    """Check if a file is currently being tracked by git"""
    import subprocess

    try:
        result = subprocess.run(
            ['git', 'ls-files', path],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        return bool(result.stdout.strip())
    except subprocess.CalledProcessError:
        return False


def git_initialize_repo(repo_root: str) -> None:
    import os

    if not os.path.isdir(repo_root):
        os.makedirs(repo_root, exist_ok=True)

    cmd = ['git', 'init']
    run_git_command(cmd, repo_root=repo_root)


def git_is_repo_root(repo_root: str) -> bool:
    import os

    return os.path.isdir(os.path.join(repo_root, '.git'))


def git_add_file(path: str, repo_root: str) -> None:
    cmd = ['git', 'add', path]

    run_git_command(cmd, repo_root=repo_root)


def git_add_and_commit_file(
    path: str, repo_root: str, message: str | None = None
) -> None:
    if message is None:
        import os

        message = 'Add ' + os.path.relpath(path, repo_root)

    git_add_file(path=path, repo_root=repo_root)
    git_commit(message=message, repo_root=repo_root)


def git_remove_file(path: str, repo_root: str) -> None:
    cmd = ['git', 'rm', path]

    run_git_command(cmd, repo_root=repo_root)


def git_remove_and_commit_file(
    path: str, repo_root: str, message: str | None = None
) -> None:
    if message is None:
        import os

        message = 'Remove ' + os.path.relpath(path, repo_root)

    if git_is_file_tracked(path, repo_root=repo_root):
        git_remove_file(path=path, repo_root=repo_root)
        git_commit(message=message, repo_root=repo_root)


def git_commit(message: str, repo_root: str, *, verbose: bool = False) -> None:
    """Commit staged changes"""
    import subprocess

    if not git_is_in_repo(repo_root):
        return

    # Check if there are staged changes
    try:
        result = subprocess.run(
            ['git', 'diff', '--cached', '--name-only'],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
        if not result.stdout.strip():
            # No staged changes
            if verbose:
                print('Nothing to commit, working tree clean')
            return
    except subprocess.CalledProcessError as e:
        # If git diff fails, log it but continue with commit attempt
        print(f'Warning: Could not check for staged changes: {e}')

    # Proceed with commit
    try:
        result = subprocess.run(
            ['git', 'commit', '-m', message],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        if verbose:
            print(f'Committed: {result.stdout.strip()}')
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.strip() if e.stderr else str(e)
        print(f'Error committing: {error_msg}')
        raise


def run_git_command(cmd: list[str], repo_root: str) -> str | None:
    """Run a git command in the specified repository directory"""
    import subprocess

    if not git_is_in_repo(repo_root):
        return None

    try:
        result = subprocess.run(
            cmd,
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.strip() if e.stderr else str(e)
        print(f"Error running command '{cmd}': {error_msg}")
        raise
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
        raise
