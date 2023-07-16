from __future__ import annotations

import pytest

import polars as pl
from polars.testing import assert_frame_equal
from polars.utils.lambda_parser import _parse_str_to_ast_lambda, _process_ast_expr


@pytest.mark.parametrize(
    "func",
    [
        "lambda x: x[0] + 1",
        "lambda x: x[0] + x[1] - x[2] / x[0] ** x[1]",
        "lambda x: x[0] // x[1] % x[0]",
        "lambda x: x[2] & x[3]",
        "lambda x: x[2] | x[3]",
        # "lambda x: x[2] == x[3]",  # todo
        # "lambda x: x[2] != x[3]",  # todo
    ],
)
def test_dataframe_apply_produces_warning(func: str) -> None:
    df = pl.DataFrame(
        {
            "a": [1.0, 1.0, 3.1],
            "b": [4, 5, 6],
            "c": [True, True, False],
            "d": [False, True, True],
        }
    )
    ast_lambda = _parse_str_to_ast_lambda(func)
    assert ast_lambda is not None
    out = _process_ast_expr(
        ast_lambda.body, ast_lambda.args.args[0].arg, df.columns, level=0, is_expr=False
    )
    assert out is not None
    result = df.select(eval(out).alias("apply"))
    expected = df.apply(eval(func))
    assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "func",
    [
        "lambda x: np.sin(x)",
        "np.sin",
        "lambda x, y: x+y",
    ],
)
def test_non_simple_lambda(func: str) -> None:
    ast_lambda = _parse_str_to_ast_lambda(func)
    assert ast_lambda is None


@pytest.mark.parametrize(
    "func",
    [
        "lambda x: x+1",
        "lambda x: x[0]+np.sin(x)",
        "lambda x: a[0]+1",
        "lambda x: x[b+1]+1",
        'lambda x: x["a"]+1',
    ],
)
def test_dataframe_apply_noop(func: str) -> None:
    df = pl.DataFrame(
        {
            "a": [1, 1, 3],
            "b": [4, 5, 6],
            "c": [True, True, False],
            "d": [False, True, True],
        }
    )
    ast_lambda = _parse_str_to_ast_lambda(func)
    assert ast_lambda is not None
    out = _process_ast_expr(
        ast_lambda.body, ast_lambda.args.args[0].arg, df.columns, level=0, is_expr=False
    )
    assert out is None


@pytest.mark.parametrize(
    "func",
    [
        "lambda x: x + 1",
        "lambda x: x - 1",
        "lambda x: x * 1",
        "lambda x: x / 1",
        "lambda x: x // 1",
        "lambda x: x % 2",
        "lambda x: x & True",
        "lambda x: x | False",
        # "lambda x: x[2] == x[3]",  # todo
        # "lambda x: x[2] != x[3]",  # todo
    ],
)
def test_expr_apply_produces_warning(func: str) -> None:
    df = pl.DataFrame(
        {
            "a": [1, 1, 3],
            "b": [4, 5, 6],
            "c": [True, True, False],
            "d": [False, True, True],
        }
    )
    ast_lambda = _parse_str_to_ast_lambda(func)
    assert ast_lambda is not None
    out = _process_ast_expr(
        ast_lambda.body, ast_lambda.args.args[0].arg, ["a"], level=0, is_expr=True
    )
    assert out is not None
    result = df.select(eval(out))
    expected = df.select(pl.col("a").apply(eval(func)))
    assert_frame_equal(result, expected)


def test_expr_apply_non_binary_operator() -> None:
    ast_lambda = _parse_str_to_ast_lambda("lambda x: np.sin(x)")
    assert ast_lambda is None


def test_expr_apply_subscripts() -> None:
    ast_lambda = _parse_str_to_ast_lambda("lambda x: x[0] + 1")
    assert ast_lambda is not None
    out = _process_ast_expr(
        ast_lambda.body, ast_lambda.args.args[0].arg, ["a"], level=0, is_expr=True
    )
    assert out is None
