use arrow::array::PrimitiveArray;
use arrow::compute::arity::unary;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use arrow::temporal_conversions::{
    timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_us_to_datetime,
    parse_offset,
};

use crate::prelude::ArrayRef;
use super::*;
use crate::prelude::*;
use chrono::FixedOffset;

#[cfg(feature = "timezones")]
pub fn cast_timezone(
    arr: &PrimitiveArray<i64>,
    tu: TimeUnit,
    from: String,
    to: String,
) -> ArrayRef {
    use chrono::TimeZone;

    match tu {
        TimeUnit::Microsecond => Box::new(unary(
            arr,
            |value| {
                println!("actually got here");
                let ndt = timestamp_us_to_datetime(value);
                // let tz_aware = from.from_local_datetime(&ndt).unwrap();
                let tz_aware = parse_offset(&from).unwrap().from_utc_datetime(&ndt);
                let newto = parse_offset(&to).unwrap();
                let new_tz_aware = tz_aware.with_timezone(&newto);
                new_tz_aware.naive_local().timestamp_micros()
            },
            ArrowDataType::Int64,
        )),
        _ => unreachable!(),
    }
}
