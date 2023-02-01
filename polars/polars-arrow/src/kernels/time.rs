use arrow::array::PrimitiveArray;
use arrow::compute::arity::unary;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use arrow::temporal_conversions::{
    parse_offset, timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_us_to_datetime,
};

use crate::prelude::ArrayRef;
use crate::error::Result;
use crate::error::PolarsError;
use chrono::FixedOffset;
use chrono::NaiveDateTime;
use chrono_tz::Tz;
use chrono::TimeZone;

fn from_fixed_offset_to_tz( from_tz: FixedOffset, to_tz: Tz, ndt: NaiveDateTime,) -> NaiveDateTime {
    from_tz.from_local_datetime(&ndt)
    .unwrap()
    .with_timezone(&to_tz)
    .naive_local()
}
fn from_fixed_offset_to_fixed_offset( from_tz: FixedOffset, to_tz: FixedOffset, ndt: NaiveDateTime,) -> NaiveDateTime {
    from_tz.from_local_datetime(&ndt)
    .unwrap()
    .with_timezone(&to_tz)
    .naive_local()
}
fn from_tz_to_fixed_offset( from_tz: Tz, to_tz: FixedOffset, ndt: NaiveDateTime,) -> NaiveDateTime {
    from_tz.from_local_datetime(&ndt)
    .unwrap()
    .with_timezone(&to_tz)
    .naive_local()
}
fn from_tz_to_tz( from_tz: Tz, to_tz: Tz, ndt: NaiveDateTime,) -> NaiveDateTime {
    from_tz.from_local_datetime(&ndt)
    .unwrap()
    .with_timezone(&to_tz)
    .naive_local()
}
// fn convert_millis(value: i64, op: fn(NaiveDateTime)->NaiveDateTime) -> i64{
//     let ndt = timestamp_ms_to_datetime(value);
//     op(ndt).timestamp_millis()
// }
fn convert_micros(value: i64, op: impl Fn(NaiveDateTime)->NaiveDateTime) -> i64{
    let ndt = timestamp_us_to_datetime(value);
    op(ndt).timestamp_micros()
}
// fn convert_nanos(value: i64, op: fn(NaiveDateTime)->NaiveDateTime) -> i64{
//     let ndt = timestamp_ns_to_datetime(value);
//     op(ndt).timestamp_nanos()
// }


#[cfg(feature = "timezones")]
pub fn cast_timezone(
    arr: &PrimitiveArray<i64>,
    tu: TimeUnit,
    from: String,
    to: String,
) -> Result<ArrayRef> {
    use chrono::TimeZone;

    match from.parse::<chrono_tz::Tz>() {
        Ok(from_tz) => match to.parse::<chrono_tz::Tz>() {
            Ok(to_tz) => {
                Ok(Box::new(unary(arr, |value| {convert_micros(value, |value|{from_tz_to_tz(from_tz, to_tz, value)})}, ArrowDataType::Int64)))
            }
            Err(_) => match parse_offset(&to) {
                Ok(to_tz) => {
                    Ok(Box::new(unary(arr, |value| {convert_micros(value, |value|{from_tz_to_fixed_offset(from_tz, to_tz, value)})}, ArrowDataType::Int64)))
                }
                Err(_) => Err(PolarsError::ComputeError(
                    "only allowed for child arrays without nulls".into(),
                ))
            }
        },
        Err(_) => Err(PolarsError::ComputeError(
            "only allowed for child arrays without nulls".into(),
        ))
    }
}