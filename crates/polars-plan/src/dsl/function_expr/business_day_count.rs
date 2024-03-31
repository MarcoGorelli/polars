use polars_core::prelude::*;
use polars_time::{business_day_count_impl};

pub(super) fn business_day_count(
    s: &[Series],
) -> PolarsResult<Series> {
    let start = &s[0];
    let end = &s[1];
    Ok(business_day_count_impl(&start.date()?.0, &end.date()?.0)?.into_series())
}
