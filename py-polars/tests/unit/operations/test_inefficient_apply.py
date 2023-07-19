from __future__ import annotations

from typing import Any, Callable

import numpy as np
import pytest

import polars as pl
from polars.exceptions import PolarsInefficientApplyWarning
from polars.utils.udfs import (
    _bytecode_to_expression,
    _can_rewrite_as_expression,
    _get_bytecode_ops,
    _param_name_from_signature,
)

MY_CONSTANT = 3
MY_GLOBAL_DICT = {"1": 1, "2": 2, "3": 3}


def _get_suggestion(
    func: Callable[[Any], Any], col: str, apply_target: str, param_name: str
) -> str | None:
    return _bytecode_to_expression(
        _get_bytecode_ops(func), col, apply_target, param_name
    )


@pytest.mark.parametrize(
    "func",
    [
        np.sin,
        lambda x: np.sin(x),
        lambda x, y: x + y,
        lambda x: x[0] + 1,
        lambda x: x,
        lambda x: x > 0 and (x < 100 or (x % 2 == 0)),
    ],
)
def test_non_simple_function(func: Callable[[Any], Any]) -> None:
    assert not _param_name_from_signature(func) or not _can_rewrite_as_expression(
        _get_bytecode_ops(func), apply_target="expr", param_name="x"
    )


@pytest.mark.parametrize(
    "func",
    [
        lambda x: x + 1 - (2 / 3),
        lambda x: x // 1 % 2,
        lambda x: x & True,
        lambda x: x | False,
        lambda x: x != 3,
        lambda x: x > 1,
        lambda x: not (x > 1) or x == 2,
        lambda x: x is None,
        lambda x: x is not None,
        lambda x: (x * -x) ** x,
        lambda x: x * (x**x),
        lambda x: (x / x) + ((x * x) - x),
        lambda x: (10 - x) / (((x * 4) - x) // (2 + (x * (x - 1)))),
        lambda x: x in (2, 3, 4),
        lambda x: x not in (2, 3, 4),
        lambda x: x in (1, 2, 3, 4, 3) and x % 2 == 0 and x > 0,
        lambda x: MY_CONSTANT + x,
    ],
)
def test_expr_apply_produces_warning(func: Callable[[Any], Any]) -> None:
    with pytest.warns(
        PolarsInefficientApplyWarning, match="In this case, you can replace"
    ):
        suggestion = _get_suggestion(func, col="a", apply_target="expr", param_name="x")
        assert suggestion is not None

        df = pl.DataFrame({"a": [1, 2, 3]})
        result = df.select(
            x="a",
            y=eval(suggestion),
        )
        expected = df.select(
            x=pl.col("a"),
            y=pl.col("a").apply(func),
        )
        assert result.rows() == expected.rows()


def test_expr_apply_produces_warning_misc() -> None:
    # note: can also identify inefficient functions and methods as well as lambdas
    class Test:
        def x10(self, x: pl.Expr) -> pl.Expr:
            return x * 10

    suggestion = _get_suggestion(
        Test().x10, col="colx", apply_target="expr", param_name="x"
    )
    assert suggestion == 'pl.col("colx") * 10'


def test_map_dict() -> None:
    my_local_dict = {"1": 1, "2": 2, "3": 3}
    for func in [
        lambda x: my_local_dict[x],
        lambda x: MY_GLOBAL_DICT[x],
    ]:
        suggestion = _get_suggestion(func, "a", "expr", "x")
        assert suggestion is not None

        df = pl.DataFrame({"a": ["1", "2", "3"]})
        result = df.select(
            x="a",
            y=eval(suggestion),
        )
        with pytest.warns(
            PolarsInefficientApplyWarning, match="In this case, you can replace"
        ):
            expected = df.select(
                x=pl.col("a"),
                y=pl.col("a").apply(func),
            )
        assert result.rows() == expected.rows()
