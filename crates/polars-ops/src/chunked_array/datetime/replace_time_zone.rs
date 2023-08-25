use arrow::temporal_conversions::{
    timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_us_to_datetime,
};
use chrono::NaiveDateTime;
use chrono_tz::Tz;
use polars_arrow::kernels::convert_to_naive_local;
use polars_core::chunked_array::ops::arity::try_binary_elementwise_values;
use polars_core::prelude::*;

fn parse_time_zone(s: &str) -> PolarsResult<Tz> {
    s.parse()
        .map_err(|e| polars_err!(ComputeError: format!("unable to parse time zone: '{s}': {e}")))
}

pub fn replace_time_zone(
    datetime: &Logical<DatetimeType, Int64Type>,
    time_zone: Option<&str>,
    ambiguous: &Utf8Chunked,
) -> PolarsResult<DatetimeChunked> {
    let from_tz = parse_time_zone(datetime.time_zone().as_deref().unwrap_or("UTC"))?;
    let to_tz = parse_time_zone(time_zone.unwrap_or("UTC"))?;
    let timestamp_to_datetime: fn(i64) -> NaiveDateTime = match datetime.time_unit() {
        TimeUnit::Milliseconds => timestamp_ms_to_datetime,
        TimeUnit::Microseconds => timestamp_us_to_datetime,
        TimeUnit::Nanoseconds => timestamp_ns_to_datetime,
    };
    let datetime_to_timestamp: fn(NaiveDateTime) -> i64 = match datetime.time_unit() {
        TimeUnit::Milliseconds => datetime_to_timestamp_ms,
        TimeUnit::Microseconds => datetime_to_timestamp_us,
        TimeUnit::Nanoseconds => datetime_to_timestamp_ns,
    };
    let out = match ambiguous.len() {
        1 => match ambiguous.get(0) {
            Some(ambiguous) => datetime.0.try_apply(|timestamp| {
                let ndt = timestamp_to_datetime(timestamp);
                Ok(datetime_to_timestamp(convert_to_naive_local(
                    &from_tz, &to_tz, ndt, ambiguous,
                )?))
            }),
            _ => Ok(datetime.0.apply(|_| None)),
        },
        _ => {
            try_binary_elementwise_values(datetime, ambiguous, |timestamp: i64, ambiguous: &str| {
                let ndt = timestamp_to_datetime(timestamp);
                Ok::<i64, PolarsError>(datetime_to_timestamp(convert_to_naive_local(
                    &from_tz, &to_tz, ndt, ambiguous,
                )?))
            })
        },
    };
    let mut out = out?.into_datetime(datetime.time_unit(), time_zone.map(|x| x.to_string()));
    out.set_sorted_flag(datetime.is_sorted_flag());
    Ok(out)
}
