use super::*;
use polars::lazy::dsl;
use polars_core::with_match_physical_integer_polars_type;
use pyo3::prelude::*;

use crate::error::PyPolarsErr;
use crate::prelude::*;
use crate::{PyExpr, PySeries};

#[pyfunction]
pub fn business_day_count(
    start: PyExpr,
    end: PyExpr,
) -> PyExpr {
    let start = start.inner;
    let end = end.inner;
    dsl::business_day_count(start, end).into()
}