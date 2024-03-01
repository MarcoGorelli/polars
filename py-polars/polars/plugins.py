from __future__ import annotations

from pathlib import Path

from polars._utils.unstable import unstable

__all__ = ["get_shared_lib_location"]


@unstable()
def get_shared_lib_location(package_init_path: str) -> str:
    """
    Get location of Shared Object file.

    .. warning::
        This functionality is considered **unstable**. It may be changed
        at any point without it being considered a breaking change.

    Parameters
    ----------
    package_init_path
        The ``__init__.py`` file of the plugin package.

    Returns
    -------
    str
        The location of the Shared Object file.
    """
    directory = Path(package_init_path).parent
    return str(directory / next(filter(_is_shared_lib, directory.iterdir())))


def _is_shared_lib(file: Path) -> bool:
    return file.name.endswith((".so", ".dll", ".pyd"))
