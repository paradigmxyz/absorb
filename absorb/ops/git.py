from __future__ import annotations


def git_is_file_tracked(path: str, repo_root: str) -> bool:
    """Check if a file is currently being tracked by git"""
    import subprocess

    try:
        # Method 1: Use git ls-files to check if file is tracked
        result = subprocess.run(
            ['git', 'ls-files', path],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        # If the file is tracked, git ls-files returns the filename
        # If not tracked, it returns empty string
        return bool(result.stdout.strip())
    except subprocess.CalledProcessError:
        # If git command fails, assume file is not tracked
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

    git_remove_file(path=path, repo_root=repo_root)
    git_commit(message=message, repo_root=repo_root)


def git_commit(message: str, repo_root: str) -> None:
    """Commit staged changes"""
    # Use list format to handle messages with spaces/quotes safely
    import subprocess

    try:
        result = subprocess.run(
            ['git', 'commit', '-m', message],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        print(f'Committed: {result.stdout.strip()}')
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.strip() if e.stderr else str(e)
        print(f'Error committing: {error_msg}')
        raise


def run_git_command(cmd: list[str], repo_root: str) -> str:
    """Run a git command in the specified repository directory"""
    import subprocess

    try:
        result = subprocess.run(
            cmd,
            cwd=repo_root,  # Use cwd parameter instead of cd &&
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
