"""Deprecated module. Do not use."""

import os
from typing import Any

from polars._utils.deprecation import issue_deprecation_warning

__all__ = ["_get_shared_lib_location"]


def _get_shared_lib_location(main_file: Any) -> str:
    issue_deprecation_warning(
        "polars.utils will be made private in a future release. Please use "
        "`from polars.plugins import register_plugin` instead. "
        "Note that its interface has changed - check the user guide "
        "(https://docs.pola.rs/user-guide/expressions/plugins) "
        "for the currently-recommended way to register a plugin.",
        version="0.20.14",
    )
    directory = os.path.dirname(main_file)  # noqa: PTH120
    return os.path.join(  # noqa: PTH118
        directory, next(filter(_is_shared_lib, os.listdir(directory)))
    )


def _is_shared_lib(file: str) -> bool:
    return file.endswith((".so", ".dll", ".pyd"))
