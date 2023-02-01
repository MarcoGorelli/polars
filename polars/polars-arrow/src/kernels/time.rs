use arrow::array::PrimitiveArray;
use arrow::compute::arity::unary;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use arrow::temporal_conversions::{
    parse_offset, timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_us_to_datetime,
};
#[cfg(feature = "timezones")]
use chrono::{FixedOffset, NaiveDateTime, TimeZone};
#[cfg(feature = "timezones")]
use chrono_tz::Tz;

use crate::prelude::ArrayRef;

#[cfg(feature = "timezones")]
fn from_fixed_offset_to_tz(from_tz: FixedOffset, to_tz: Tz, ndt: NaiveDateTime) -> NaiveDateTime {
    from_tz
        .from_local_datetime(&ndt)
        .unwrap()
        .with_timezone(&to_tz)
        .naive_local()
}
#[cfg(feature = "timezones")]
fn from_fixed_offset_to_fixed_offset(
    from_tz: FixedOffset,
    to_tz: FixedOffset,
    ndt: NaiveDateTime,
) -> NaiveDateTime {
    from_tz
        .from_local_datetime(&ndt)
        .unwrap()
        .with_timezone(&to_tz)
        .naive_local()
}
#[cfg(feature = "timezones")]
fn from_tz_to_fixed_offset(from_tz: Tz, to_tz: FixedOffset, ndt: NaiveDateTime) -> NaiveDateTime {
    from_tz
        .from_local_datetime(&ndt)
        .unwrap()
        .with_timezone(&to_tz)
        .naive_local()
}
#[cfg(feature = "timezones")]
fn from_tz_to_tz(from_tz: Tz, to_tz: Tz, ndt: NaiveDateTime) -> NaiveDateTime {
    from_tz
        .from_local_datetime(&ndt)
        .unwrap()
        .with_timezone(&to_tz)
        .naive_local()
}
#[cfg(feature = "timezones")]
fn from_to<T1: TimeZone, T2: TimeZone>(from_tz: T1, to_tz: T2, ndt: NaiveDateTime) -> impl Fn(NaiveDateTime) -> NaiveDateTime {
    fn inner<T1: TimeZone, T2: TimeZone>(from_tz: T1, to_tz: T2, ndt: NaiveDateTime) -> NaiveDateTime{
        from_tz
        .from_local_datetime(&ndt)
        .unwrap()
        .with_timezone(&to_tz)
        .naive_local()
    }
    |ndt| inner(from_tz, to_tz, ndt)
}
fn convert_millis(value: i64, op: impl Fn(NaiveDateTime) -> NaiveDateTime) -> i64 {
    let ndt = timestamp_ms_to_datetime(value);
    op(ndt).timestamp_millis()
}
fn convert_micros(value: i64, op: impl Fn(NaiveDateTime) -> NaiveDateTime) -> i64 {
    let ndt = timestamp_us_to_datetime(value);
    op(ndt).timestamp_micros()
}
fn convert_nanos(value: i64, op: impl Fn(NaiveDateTime) -> NaiveDateTime) -> i64 {
    let ndt = timestamp_ns_to_datetime(value);
    op(ndt).timestamp_nanos()
}


#[cfg(feature = "timezones")]
pub fn cast_timezone(
    arr: &PrimitiveArray<i64>,
    tu: TimeUnit,
    from: String,
    to: String,
) -> ArrayRef {
    let conversion_func2 = match tu {
        TimeUnit::Millisecond => convert_millis,
        TimeUnit::Microsecond => convert_micros,
        TimeUnit::Nanosecond => convert_nanos,
        _ => unreachable!(),
    };
    match from.parse::<chrono_tz::Tz>() {
        Ok(from_tz) => match to.parse::<chrono_tz::Tz>() {
            Ok(to_tz) => {
                Box::new(unary(
                    arr,
                    |value| conversion_func2(value, from_to(from_tz, to_tz)),
                    ArrowDataType::Int64,
                ))
            }
            Err(_) => match parse_offset(&to) {
                Ok(to_tz) => {
                    Box::new(unary(
                        arr,
                        |value| conversion_func2(value, from_to(from_tz, to_tz)),
                        ArrowDataType::Int64,
                    ))
                }
                Err(_) => panic!("Could not parse timezone {to}"),
            },
        },
        Err(_) => match parse_offset(&from) {
            Ok(from_tz) => match to.parse::<chrono_tz::Tz>() {
                Ok(to_tz) => {
                    let conversion_func = match tu {
                        TimeUnit::Millisecond => convert_millis,
                        TimeUnit::Microsecond => convert_micros,
                        TimeUnit::Nanosecond => convert_nanos,
                        _ => unreachable!(),
                    };
                    Box::new(unary(
                        arr,
                        |value| {
                            conversion_func(value, |value| {
                                from_fixed_offset_to_tz(from_tz, to_tz, value)
                            })
                        },
                        ArrowDataType::Int64,
                    ))
                }
                Err(_) => match parse_offset(&to) {
                    Ok(to_tz) => {
                        let conversion_func = match tu {
                            TimeUnit::Millisecond => convert_millis,
                            TimeUnit::Microsecond => convert_micros,
                            TimeUnit::Nanosecond => convert_nanos,
                            _ => unreachable!(),
                        };
                        Box::new(unary(
                            arr,
                            |value| {
                                conversion_func(value, |value| {
                                    from_fixed_offset_to_fixed_offset(from_tz, to_tz, value)
                                })
                            },
                            ArrowDataType::Int64,
                        ))
                    }
                    Err(_) => panic!("Could not parse timezone {to}"),
                },
            },
            Err(_) => panic!("Could not parse timezone {from}"),
        },
    }
}
