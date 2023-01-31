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
        TimeUnit::Microsecond => Box::new(unary(
            arr,
            |value| {
                println!("actually got here");
                let ndt = timestamp_us_to_datetime(value);
                // let tz_aware = from.from_local_datetime(&ndt).unwrap();
                // let tz_aware = parse_offset(&from).unwrap().from_utc_datetime(&ndt);
                // so, we need to do a kind of try-except for this newto
                match from.parse::<chrono_tz::Tz>() {
                    Ok(from_tz) => {
                        match to.parse::<chrono_tz::Tz>() {
                            Ok(to_tz) => {
                                tz.from_utc_datetime(&ndt).with_timezone(&tz).naive_local().timestamp_micros()
                            }
                            Err(_) => match parse_offset(&to) {
                                Ok(to_tz) => {
                                    tz.from_utc_datetime(&ndt).with_timezone(&tz).naive_local().timestamp_micros()
                                    // tz_aware.with_timezone(&tz).naive_local().timestamp_micros()
                                }
                                Err(_) => unreachable!(),
                            },
                        }
                        // tz.from_utc_datetime(&ndt)
                    }
                    Err(_) => match parse_offset(&from) {
                        Ok(from_tz) => {
                            match to.parse::<chrono_tz::Tz>() {
                                Ok(to_tz) => {
                                    tz.from_utc_datetime(&ndt).with_timezone(&tz).naive_local().timestamp_micros()
                                }
                                Err(_) => match parse_offset(&to) {
                                    Ok(to_tz) => {
                                        tz.from_utc_datetime(&ndt).with_timezone(&tz).naive_local().timestamp_micros()
                                        // tz_aware.with_timezone(&tz).naive_local().timestamp_micros()
                                    }
                                    Err(_) => unreachable!(),
                                },
                            }
                            // tz.from_utc_datetime(&ndt)
                        }
                        Err(_) => unreachable!(),
                    },
                }
                // match to.parse::<chrono_tz::Tz>() {
                //     Ok(tz) => {
                //         tz_aware.with_timezone(&tz).naive_local().timestamp_micros()
                //     }
                //     Err(_) => match parse_offset(&to) {
                //         Ok(tz) => {
                //             tz_aware.with_timezone(&tz).naive_local().timestamp_micros()
                //         }
                //         Err(_) => unreachable!(),
                //     },
                // }
                // let newto = parse_offset(&to).unwrap();
                // let new_tz_aware = tz_aware.with_timezone(&newto);
                // new_tz_aware.naive_local().timestamp_micros()
            },
            ArrowDataType::Int64,
        )),
        _ => unreachable!(),
    }
}
