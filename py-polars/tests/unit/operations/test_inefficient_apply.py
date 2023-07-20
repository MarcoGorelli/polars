from __future__ import annotations

from typing import Any, Callable

import numpy as np
import pytest

import polars as pl
from polars.exceptions import PolarsInefficientApplyWarning
from polars.utils.various import find_stacklevel
from polars.utils.udfs import BytecodeParser

MY_CONSTANT = 3
MY_GLOBAL_DICT = {1: 2, 2: 3, 3: 1}
MY_GLOBAL_ARRAY = [1, 2, 3]


@pytest.mark.parametrize(
    "func",
    [
        np.sin,
        lambda x: np.sin(x),
        lambda x, y: x + y,
        lambda x: x[0] + 1,
        lambda x: x,
        lambda x: x > 0 and (x < 100 or (x % 2 == 0)),
        lambda x: MY_GLOBAL_ARRAY[x],
    ],
)
def test_non_simple_function(func: Callable[[Any], Any]) -> None:
    assert not BytecodeParser(func, apply_target="expr", stacklevel=2).can_rewrite()


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
        parser = BytecodeParser(func, apply_target="expr", stacklevel=2)
        suggested_expression = parser.to_expression(col="a")
        assert suggested_expression is not None

        df = pl.DataFrame({"a": [1, 2, 3]})
        result = df.select(
            x="a",
            y=eval(suggested_expression),
        )
        expected = df.select(
            x=pl.col("a"),
            y=pl.col("a").apply(func),
        )
        assert result.rows() == expected.rows()


def test_expr_apply_parsing_misc() -> None:
    # note: can also identify inefficient functions and methods as well as lambdas
    class Test:
        def x10(self, x: pl.Expr) -> pl.Expr:
            return x * 10

    parser = BytecodeParser(Test().x10, apply_target="expr", stacklevel=2)
    suggested_expression = parser.to_expression(col="colx")
    assert suggested_expression == 'pl.col("colx") * 10'

    # note: all constants - should not create a warning/suggestion
    suggested_expression = BytecodeParser(
        lambda x: MY_CONSTANT + 42, apply_target="expr", stacklevel=2,
    ).to_expression(col="colx")
    assert suggested_expression is None


def test_map_dict() -> None:
    my_local_dict = {1: 2, 2: 3, 3: 1}
    for func in [
        lambda x: my_local_dict[x],
        lambda x: MY_GLOBAL_DICT[x],
    ]:
        suggestion = BytecodeParser(func, apply_target="expr", stacklevel=2).to_expression(col='a')
        assert suggestion is not None

        df = pl.DataFrame({"a": [1, 2, 3]})
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