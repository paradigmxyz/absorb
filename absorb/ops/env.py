from __future__ import annotations


def is_package_installed(package: str) -> bool:
    """
    Check if a package is installed, optionally with version constraints.

    Args:
        package: Package name with optional version specifier
                 e.g., 'numpy', 'numpy>=1.20', 'numpy==1.21.0'

    Returns:
        bool: True if package is installed and meets version requirements
    """
    import importlib.metadata
    import re

    # Parse package string to extract name and version specifier
    match = re.match(r'^([a-zA-Z0-9_-]+)\s*(.*)$', package.strip())
    if not match:
        return False

    package_name = match.group(1)
    version_spec = match.group(2).strip()

    try:
        # Get installed version
        installed_version = importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return False

    # If no version specifier, just check if installed
    if not version_spec:
        return True

    # Parse version specifier
    spec_match = re.match(r'^(==|!=|<=|>=|<|>|~=)\s*(.+)$', version_spec)
    if not spec_match:
        return False

    operator = spec_match.group(1)
    required_version = spec_match.group(2).strip()

    # Compare versions
    return compare_versions(installed_version, operator, required_version)


def compare_versions(installed: str, operator: str, required: str) -> bool:
    """Compare version strings."""
    import re

    # Convert version strings to tuples of integers for comparison
    def version_tuple(v: str) -> tuple[int, ...]:
        return tuple(int(x) for x in re.findall(r'\d+', v))

    installed_tuple = version_tuple(installed)
    required_tuple = version_tuple(required)

    if operator == '==':
        return installed_tuple == required_tuple
    elif operator == '!=':
        return installed_tuple != required_tuple
    elif operator == '>=':
        return installed_tuple >= required_tuple
    elif operator == '<=':
        return installed_tuple <= required_tuple
    elif operator == '>':
        return installed_tuple > required_tuple
    elif operator == '<':
        return installed_tuple < required_tuple
    elif operator == '~=':
        # Compatible release: version should be >= X.Y and < X+1.0
        if len(required_tuple) < 2:
            return False
        return (
            installed_tuple[: len(required_tuple) - 1] == required_tuple[:-1]
            and installed_tuple >= required_tuple
        )

    return False
