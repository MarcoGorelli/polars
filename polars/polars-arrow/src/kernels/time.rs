use arrow::array::PrimitiveArray;
use arrow::compute::arity::unary;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use arrow::temporal_conversions::{
    parse_offset, timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_us_to_datetime,
};

use crate::prelude::ArrayRef;

#[cfg(feature = "timezones")]
pub fn cast_timezone(
    arr: &PrimitiveArray<i64>,
    tu: TimeUnit,
    from: String,
    to: String,
) -> ArrayRef {
    use chrono::TimeZone;

    match tu {
        TimeUnit::Millisecond => Box::new(unary(
            arr,
            |value| {
                let ndt = timestamp_ms_to_datetime(value);
                match from.parse::<chrono_tz::Tz>() {
                    Ok(from_tz) => match to.parse::<chrono_tz::Tz>() {
                        Ok(to_tz) => from_tz
                            .from_local_datetime(&ndt)
                            .unwrap()
                            .with_timezone(&to_tz)
                            .naive_local()
                            .timestamp_millis(),
                        Err(_) => match parse_offset(&to) {
                            Ok(to_tz) => from_tz
                                .from_local_datetime(&ndt)
                                .unwrap()
                                .with_timezone(&to_tz)
                                .naive_local()
                                .timestamp_millis(),
                            Err(_) => panic!("Could not parse timezone {to}"),
                        },
                    },
                    Err(_) => match parse_offset(&from) {
                        Ok(from_tz) => match to.parse::<chrono_tz::Tz>() {
                            Ok(to_tz) => from_tz
                                .from_local_datetime(&ndt)
                                .unwrap()
                                .with_timezone(&to_tz)
                                .naive_local()
                                .timestamp_millis(),
                            Err(_) => match parse_offset(&to) {
                                Ok(to_tz) => from_tz
                                    .from_local_datetime(&ndt)
                                    .unwrap()
                                    .with_timezone(&to_tz)
                                    .naive_local()
                                    .timestamp_millis(),
                                Err(_) => panic!("Could not parse timezone {to}"),
                            },
                        },
                        Err(_) => panic!("Could not parse timezone {from}"),
                    },
                }
            },
            ArrowDataType::Int64,
        )),
        TimeUnit::Microsecond => Box::new(unary(
            arr,
            |value| {
                let ndt = timestamp_us_to_datetime(value);
                match from.parse::<chrono_tz::Tz>() {
                    Ok(from_tz) => match to.parse::<chrono_tz::Tz>() {
                        Ok(to_tz) => from_tz
                            .from_local_datetime(&ndt)
                            .unwrap()
                            .with_timezone(&to_tz)
                            .naive_local()
                            .timestamp_micros(),
                        Err(_) => match parse_offset(&to) {
                            Ok(to_tz) => from_tz
                                .from_local_datetime(&ndt)
                                .unwrap()
                                .with_timezone(&to_tz)
                                .naive_local()
                                .timestamp_micros(),
                            Err(_) => panic!("Could not parse timezone {to}"),
                        },
                    },
                    Err(_) => match parse_offset(&from) {
                        Ok(from_tz) => match to.parse::<chrono_tz::Tz>() {
                            Ok(to_tz) => from_tz
                                .from_local_datetime(&ndt)
                                .unwrap()
                                .with_timezone(&to_tz)
                                .naive_local()
                                .timestamp_micros(),
                            Err(_) => match parse_offset(&to) {
                                Ok(to_tz) => {
                                    println!("here we are!");
                                    println!("to tz: {:?}", to_tz);
                                    println!("from tz: {:?}", from_tz);
                                    from_tz
                                        .from_local_datetime(&ndt)
                                        .unwrap()
                                        .with_timezone(&to_tz)
                                        .naive_local()
                                        .timestamp_micros()
                                }
                                Err(_) => panic!("Could not parse timezone {to}"),
                            },
                        },
                        Err(_) => panic!("Could not parse timezone {from}"),
                    },
                }
            },
            ArrowDataType::Int64,
        )),
        TimeUnit::Nanosecond => Box::new(unary(
            arr,
            |value| {
                let ndt = timestamp_ns_to_datetime(value);
                match from.parse::<chrono_tz::Tz>() {
                    Ok(from_tz) => match to.parse::<chrono_tz::Tz>() {
                        Ok(to_tz) => from_tz
                            .from_local_datetime(&ndt)
                            .unwrap()
                            .with_timezone(&to_tz)
                            .naive_local()
                            .timestamp_nanos(),
                        Err(_) => match parse_offset(&to) {
                            Ok(to_tz) => from_tz
                                .from_local_datetime(&ndt)
                                .unwrap()
                                .with_timezone(&to_tz)
                                .naive_local()
                                .timestamp_nanos(),
                            Err(_) => panic!("Could not parse timezone {to}"),
                        },
                    },
                    Err(_) => match parse_offset(&from) {
                        Ok(from_tz) => match to.parse::<chrono_tz::Tz>() {
                            Ok(to_tz) => from_tz
                                .from_local_datetime(&ndt)
                                .unwrap()
                                .with_timezone(&to_tz)
                                .naive_local()
                                .timestamp_nanos(),
                            Err(_) => match parse_offset(&to) {
                                Ok(to_tz) => from_tz
                                    .from_local_datetime(&ndt)
                                    .unwrap()
                                    .with_timezone(&to_tz)
                                    .naive_local()
                                    .timestamp_nanos(),
                                Err(_) => panic!("Could not parse timezone {to}"),
                            },
                        },
                        Err(_) => panic!("Could not parse timezone {from}"),
                    },
                }
            },
            ArrowDataType::Int64,
        )),
        _ => unreachable!(),
    }
}
