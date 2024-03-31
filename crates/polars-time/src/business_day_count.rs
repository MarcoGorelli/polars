use polars_core::datatypes::Int32Chunked;
use polars_error::PolarsResult;

use crate::prelude::*;

pub fn business_day_count_impl(
    start_dates: &Int32Chunked,
    end_dates: &Int32Chunked,
) -> PolarsResult<Int32Chunked>{
    return Ok(end_dates - start_dates)
}