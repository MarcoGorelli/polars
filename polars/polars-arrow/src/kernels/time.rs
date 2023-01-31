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
    use chrono_tz::Tz;
    use crate::error::PolarsError;

    match tu {
        TimeUnit::Millisecond => Box::new(unary(
            arr,
            |value| {
                let ndt = timestamp_us_to_datetime(value);
                match from.parse::<chrono_tz::Tz>() {
                    Ok(from_tz) => {
                        match to.parse::<chrono_tz::Tz>() {
                            Ok(to_tz) => {
                                from_tz.from_local_datetime(&ndt).unwrap().with_timezone(&to_tz).naive_local().timestamp_millis()
                            }
                            Err(_) => match parse_offset(&to) {
                                Ok(to_tz) => {
                                    from_tz.from_local_datetime(&ndt).unwrap().with_timezone(&to_tz).naive_local().timestamp_millis()
                                }
                                Err(_) => unreachable!(),
                            },
                        }
                    }
                    Err(_) => match parse_offset(&from) {
                        Ok(from_tz) => {
                            match to.parse::<chrono_tz::Tz>() {
                                Ok(to_tz) => {
                                    from_tz.from_utc_datetime(&ndt).with_timezone(&to_tz).naive_local().timestamp_millis()
                                }
                                Err(_) => match parse_offset(&to) {
                                    Ok(to_tz) => {
                                        from_tz.from_utc_datetime(&ndt).with_timezone(&to_tz).naive_local().timestamp_millis()
                                    }
                                    Err(_) => unreachable!(),
                                },
                            }
                        }
                        Err(_) => unreachable!(),
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
                    Ok(from_tz) => {
                        match to.parse::<chrono_tz::Tz>() {
                            Ok(to_tz) => {
                                from_tz.from_local_datetime(&ndt).unwrap().with_timezone(&to_tz).naive_local().timestamp_micros()
                            }
                            Err(_) => match parse_offset(&to) {
                                Ok(to_tz) => {
                                    from_tz.from_local_datetime(&ndt).unwrap().with_timezone(&to_tz).naive_local().timestamp_micros()
                                }
                                Err(_) => unreachable!(),
                            },
                        }
                    }
                    Err(_) => match parse_offset(&from) {
                        Ok(from_tz) => {
                            match to.parse::<chrono_tz::Tz>() {
                                Ok(to_tz) => {
                                    from_tz.from_utc_datetime(&ndt).with_timezone(&to_tz).naive_local().timestamp_micros()
                                }
                                Err(_) => match parse_offset(&to) {
                                    Ok(to_tz) => {
                                        from_tz.from_utc_datetime(&ndt).with_timezone(&to_tz).naive_local().timestamp_micros()
                                    }
                                    Err(_) => unreachable!(),
                                },
                            }
                        }
                        Err(_) => unreachable!(),
                    },
                }
            },
            ArrowDataType::Int64,
        )),
        TimeUnit::Nanosecond => Box::new(unary(
            arr,
            |value| {
                let ndt = timestamp_us_to_datetime(value);
                match from.parse::<chrono_tz::Tz>() {
                    Ok(from_tz) => {
                        match to.parse::<chrono_tz::Tz>() {
                            Ok(to_tz) => {
                                from_tz.from_local_datetime(&ndt).unwrap().with_timezone(&to_tz).naive_local().timestamp_nanos()
                            }
                            Err(_) => match parse_offset(&to) {
                                Ok(to_tz) => {
                                    from_tz.from_local_datetime(&ndt).unwrap().with_timezone(&to_tz).naive_local().timestamp_nanos()
                                }
                                Err(_) => unreachable!(),
                            },
                        }
                    }
                    Err(_) => match parse_offset(&from) {
                        Ok(from_tz) => {
                            match to.parse::<chrono_tz::Tz>() {
                                Ok(to_tz) => {
                                    from_tz.from_utc_datetime(&ndt).with_timezone(&to_tz).naive_local().timestamp_nanos()
                                }
                                Err(_) => match parse_offset(&to) {
                                    Ok(to_tz) => {
                                        from_tz.from_utc_datetime(&ndt).with_timezone(&to_tz).naive_local().timestamp_nanos()
                                    }
                                    Err(_) => unreachable!(),
                                },
                            }
                        }
                        Err(_) => unreachable!(),
                    },
                }
            },
            ArrowDataType::Int64,
        )),
        _ => unreachable!(),
    }
}
