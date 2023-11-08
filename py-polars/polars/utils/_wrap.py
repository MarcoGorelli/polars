from __future__ import annotations

from typing import TYPE_CHECKING

import polars._reexport as pl
from polars.utils.various import CollectedContext

if TYPE_CHECKING:
    from polars import DataFrame, Expr, LazyFrame, Series
    from polars.polars import PyDataFrame, PyExpr, PyLazyFrame, PySeries


def wrap_df(df: PyDataFrame) -> DataFrame:
    return pl.DataFrame._from_pydf(df)


def wrap_ldf(ldf: PyLazyFrame, collected_context: CollectedContext) -> LazyFrame:
    return pl.LazyFrame._from_pyldf(ldf, collected_context=collected_context)


def wrap_s(s: PySeries) -> Series:
    return pl.Series._from_pyseries(s)


def wrap_expr(pyexpr: PyExpr) -> Expr:
    return pl.Expr._from_pyexpr(pyexpr)
