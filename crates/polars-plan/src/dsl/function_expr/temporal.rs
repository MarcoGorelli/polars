#[cfg(feature = "date_offset")]
use polars_arrow::time_zone::Tz;
use polars_core::utils::arrow::temporal_conversions::SECONDS_IN_DAY;
#[cfg(feature = "date_offset")]
use polars_time::prelude::*;
use polars_core::utils::{align_chunks_binary, combine_validities_and};
use arrow::array::{Int64Array, Utf8Array};

use super::*;

pub(super) fn datetime(
    s: &[Series],
    time_unit: &TimeUnit,
    time_zone: Option<&str>,
    use_earliest: Option<bool>,
) -> PolarsResult<Series> {
    use polars_core::export::chrono::NaiveDate;
    use polars_core::utils::CustomIterTools;

    let year = &s[0];
    let month = &s[1];
    let day = &s[2];
    let hour = &s[3];
    let minute = &s[4];
    let second = &s[5];
    let microsecond = &s[6];

    let max_len = s.iter().map(|s| s.len()).max().unwrap();

    let mut year = year.cast(&DataType::Int32)?;
    if year.len() < max_len {
        year = year.new_from_index(0, max_len)
    }
    let year = year.i32()?;

    let mut month = month.cast(&DataType::UInt32)?;
    if month.len() < max_len {
        month = month.new_from_index(0, max_len);
    }
    let month = month.u32()?;

    let mut day = day.cast(&DataType::UInt32)?;
    if day.len() < max_len {
        day = day.new_from_index(0, max_len);
    }
    let day = day.u32()?;

    let mut hour = hour.cast(&DataType::UInt32)?;
    if hour.len() < max_len {
        hour = hour.new_from_index(0, max_len);
    }
    let hour = hour.u32()?;

    let mut minute = minute.cast(&DataType::UInt32)?;
    if minute.len() < max_len {
        minute = minute.new_from_index(0, max_len);
    }
    let minute = minute.u32()?;

    let mut second = second.cast(&DataType::UInt32)?;
    if second.len() < max_len {
        second = second.new_from_index(0, max_len);
    }
    let second = second.u32()?;

    let mut microsecond = microsecond.cast(&DataType::UInt32)?;
    if microsecond.len() < max_len {
        microsecond = microsecond.new_from_index(0, max_len);
    }
    let microsecond = microsecond.u32()?;

    let ca: Int64Chunked = year
        .into_iter()
        .zip(month)
        .zip(day)
        .zip(hour)
        .zip(minute)
        .zip(second)
        .zip(microsecond)
        .map(|((((((y, m), d), h), mnt), s), us)| {
            if let (Some(y), Some(m), Some(d), Some(h), Some(mnt), Some(s), Some(us)) =
                (y, m, d, h, mnt, s, us)
            {
                NaiveDate::from_ymd_opt(y, m, d)
                    .and_then(|nd| nd.and_hms_micro_opt(h, mnt, s, us))
                    .map(|ndt| match time_unit {
                        TimeUnit::Milliseconds => ndt.timestamp_millis(),
                        TimeUnit::Microseconds => ndt.timestamp_micros(),
                        TimeUnit::Nanoseconds => ndt.timestamp_nanos(),
                    })
            } else {
                None
            }
        })
        .collect_trusted();

    let ca = match time_zone {
        #[cfg(feature = "timezones")]
        Some(_) => {
            let mut ca = ca.into_datetime(*time_unit, None);
            ca = replace_time_zone(&ca, time_zone, use_earliest)?;
            ca
        },
        _ => {
            polars_ensure!(
                time_zone.is_none() && use_earliest.is_none(),
                ComputeError: "cannot make use of the `time_zone` and `use_earliest` arguments without the 'timezones' feature enabled."
            );
            ca.into_datetime(*time_unit, None)
        },
    };

    let mut s = ca.into_series();
    s.rename("datetime");
    Ok(s)
}



fn compute_kernel2(arr_1: &Int64Array, arr_2: &Utf8Array<i64>) -> PolarsResult<Int64Array>
where
{
    let validity = combine_validities_and(arr_1.validity(), arr_2.validity());

    let values = arr_1
        .values_iter()
        .zip(arr_2.values_iter())
        .map(|(l, r)| {
            let offset = Duration::parse(r);
            Duration::add_us(&offset, *l, None)
        })
        .collect::<PolarsResult<Vec<_>>>()?
        .into();

    Ok(Int64Array::new(arr_1.data_type().clone(), values, validity))
}

#[cfg(feature = "date_offset")]
pub(super) fn date_offset(s: &[Series]) -> PolarsResult<Series> {
    let sa = &s[0];
    let sb = &s[1];

    let preserve_sortedness: bool;
    let out = match sa.dtype().clone() {
        DataType::Date => {
            let sa = sa
                .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
                .unwrap();
            preserve_sortedness = true;
            date_offset(&[sa, sb.clone()]).and_then(|s| s.cast(&DataType::Date))
        }
        DataType::Datetime(tu, tz) => {
            let ca = sa.datetime().unwrap();
            let offsets = sb.utf8().unwrap();

            let offset_fn = match tu {
                TimeUnit::Nanoseconds => Duration::add_ns,
                TimeUnit::Microseconds => Duration::add_us,
                TimeUnit::Milliseconds => Duration::add_ms,
            };

            let tz_args = match tz {
                #[cfg(feature = "timezones")]
                Some(ref tz) => tz.parse::<Tz>().ok(),
                _ => None,
            };

            let out = match offsets.len() {
                1 => {
                    let offset = match offsets.get(0) {
                        Some(offset) => Duration::parse(offset),
                        _ => Duration::new(0),
                    };
                    ca.0.try_apply(|v| offset_fn(&offset, v, tz_args.as_ref()))
                }
                _ => {
                    let (ca_1, ca_2) = align_chunks_binary(ca, offsets);
                    fn my_fn(left: &i64, right: &str) -> PolarsResult<i64> {
                        let offset = Duration::parse(right);
                        Duration::add_us(&offset, *left, None)
                    }
                    // let res = try_binary_elementwise_values(
                    //     &ca_1,
                    //     &ca_2,
                    //     my_fn
                    // );
                    // res
                    let chunks = ca_1
                        .downcast_iter()
                        .zip(ca_2.downcast_iter())
                        .map(|
                            (arr_1, arr_2)|
                            compute_kernel2(arr_1, arr_2)
                        );
                    ChunkedArray::try_from_chunk_iter(ca_1.name(), chunks)
                    // let out = ca
                    //     .into_iter()
                    //     .zip(offsets.into_iter())
                    //     .map(|(v, offset)| {
                    //         let offset = match offset {
                    //             Some(offset) => Duration::parse(offset),
                    //             _ => Duration::new(0),
                    //         };
                    //         offset_fn(&offset, v.unwrap(), tz_args.as_ref()).unwrap()
                    //     })
                    //     .collect::<Vec<_>>();
                    // Ok(Int64Chunked::from_vec("", out))

                    // let values = lhs
                    //     .values_iter()
                    //     .zip(rhs.values_iter())
                    //     .map(|(l, r)| my_fn(*l, *r))
                    //     .collect::<Result<Vec<_>>>()?
                    //     .into();

                    // Ok(PrimitiveArray::<i64>::new(data_type, values, validity))
                }
            }?;
            // Sortedness may not be preserved when crossing daylight savings time boundaries
            // for calendar-aware durations.
            // Constant durations (e.g. 2 hours) always preserve sortedness.
            preserve_sortedness = match offsets.len() {
                1 => {
                    let offset = match offsets.get(0) {
                        Some(offset) => Duration::parse(offset),
                        _ => Duration::new(0),
                    };
                    tz.is_none() || tz.as_deref() == Some("UTC") || offset.is_constant_duration()
                }
                _ => false,
            };
            out.cast(&DataType::Datetime(tu, tz))
        },
        dt => polars_bail!(
            ComputeError: "cannot use 'date_offset' on Series of datatype {}", dt,
        ),
    };
    if preserve_sortedness {
        out.map(|mut out| {
            out.set_sorted_flag(sa.is_sorted_flag());
            out
        })
    } else {
        out
    }
}

pub(super) fn combine(s: &[Series], tu: TimeUnit) -> PolarsResult<Series> {
    let date = &s[0];
    let time = &s[1];

    let tz = match date.dtype() {
        DataType::Date => None,
        DataType::Datetime(_, tz) => tz.as_ref(),
        _dtype => {
            polars_bail!(ComputeError: format!("expected Date or Datetime, got {}", _dtype))
        },
    };

    let date = date.cast(&DataType::Date)?;
    let datetime = date.cast(&DataType::Datetime(tu, None)).unwrap();

    let duration = time.cast(&DataType::Duration(tu))?;
    let result_naive = datetime + duration;
    match tz {
        #[cfg(feature = "timezones")]
        Some(tz) => Ok(polars_ops::prelude::replace_time_zone(
            result_naive.datetime().unwrap(),
            Some(tz),
            None,
        )?
        .into()),
        _ => Ok(result_naive),
    }
}

pub(super) fn temporal_range_dispatch(
    s: &[Series],
    name: &str,
    every: Duration,
    closed: ClosedWindow,
    time_unit: Option<TimeUnit>,
    time_zone: Option<TimeZone>,
) -> PolarsResult<Series> {
    let start = &s[0];
    let stop = &s[1];

    polars_ensure!(
        start.len() == stop.len(),
        ComputeError: "'start' and 'stop' should have the same length",
    );
    const TO_MS: i64 = SECONDS_IN_DAY * 1000;

    // Note: `start` and `stop` have already been cast to their supertype,
    // so only `start`'s dtype needs to be matched against.
    #[allow(unused_mut)] // `dtype` is mutated within a "feature = timezones" block.
    let mut dtype = match (start.dtype(), time_unit) {
        (DataType::Date, time_unit) => {
            let nsecs = every.nanoseconds();
            if nsecs == 0 {
                DataType::Date
            } else if let Some(tu) = time_unit {
                DataType::Datetime(tu, None)
            } else if nsecs % 1_000 != 0 {
                DataType::Datetime(TimeUnit::Nanoseconds, None)
            } else {
                DataType::Datetime(TimeUnit::Microseconds, None)
            }
        },
        (DataType::Time, _) => DataType::Time,
        // overwrite nothing, keep as-is
        (DataType::Datetime(_, _), None) => start.dtype().clone(),
        // overwrite time unit, keep timezone
        (DataType::Datetime(_, tz), Some(tu)) => DataType::Datetime(tu, tz.clone()),
        _ => unreachable!(),
    };

    let (mut start, mut stop) = match dtype {
        #[cfg(feature = "timezones")]
        DataType::Datetime(_, Some(_)) => (
            polars_ops::prelude::replace_time_zone(
                start.cast(&dtype)?.datetime().unwrap(),
                None,
                None,
            )?
            .into_series()
            .to_physical_repr()
            .cast(&DataType::Int64)?,
            polars_ops::prelude::replace_time_zone(
                stop.cast(&dtype)?.datetime().unwrap(),
                None,
                None,
            )?
            .into_series()
            .to_physical_repr()
            .cast(&DataType::Int64)?,
        ),
        _ => (
            start
                .cast(&dtype)?
                .to_physical_repr()
                .cast(&DataType::Int64)?,
            stop.cast(&dtype)?
                .to_physical_repr()
                .cast(&DataType::Int64)?,
        ),
    };

    if dtype == DataType::Date {
        start = &start * TO_MS;
        stop = &stop * TO_MS;
    }

    // overwrite time zone, if specified
    match (&dtype, &time_zone) {
        #[cfg(feature = "timezones")]
        (DataType::Datetime(tu, _), Some(tz)) => {
            dtype = DataType::Datetime(*tu, Some(tz.clone()));
        },
        _ => {},
    };

    let start = start.get(0).unwrap().extract::<i64>().unwrap();
    let stop = stop.get(0).unwrap().extract::<i64>().unwrap();

    let out = match dtype {
        DataType::Date => date_range_impl(
            name,
            start,
            stop,
            every,
            closed,
            TimeUnit::Milliseconds,
            None,
        )?,
        DataType::Datetime(tu, ref tz) => {
            date_range_impl(name, start, stop, every, closed, tu, tz.as_ref())?
        },
        DataType::Time => date_range_impl(
            name,
            start,
            stop,
            every,
            closed,
            TimeUnit::Nanoseconds,
            None,
        )?,
        _ => unimplemented!(),
    };
    Ok(out.cast(&dtype).unwrap().into_series())
}

pub(super) fn temporal_ranges_dispatch(
    s: &[Series],
    name: &str,
    every: Duration,
    closed: ClosedWindow,
    time_unit: Option<TimeUnit>,
    time_zone: Option<TimeZone>,
) -> PolarsResult<Series> {
    let start = &s[0];
    let stop = &s[1];

    polars_ensure!(
        start.len() == stop.len(),
        ComputeError: "'start' and 'stop' should have the same length",
    );
    const TO_MS: i64 = SECONDS_IN_DAY * 1000;

    // Note: `start` and `stop` have already been cast to their supertype,
    // so only `start`'s dtype needs to be matched against.
    #[allow(unused_mut)] // `dtype` is mutated within a "feature = timezones" block.
    let mut dtype = match (start.dtype(), time_unit) {
        (DataType::Date, time_unit) => {
            let nsecs = every.nanoseconds();
            if nsecs == 0 {
                DataType::Date
            } else if let Some(tu) = time_unit {
                DataType::Datetime(tu, None)
            } else if nsecs % 1_000 != 0 {
                DataType::Datetime(TimeUnit::Nanoseconds, None)
            } else {
                DataType::Datetime(TimeUnit::Microseconds, None)
            }
        },
        (DataType::Time, _) => DataType::Time,
        // overwrite nothing, keep as-is
        (DataType::Datetime(_, _), None) => start.dtype().clone(),
        // overwrite time unit, keep timezone
        (DataType::Datetime(_, tz), Some(tu)) => DataType::Datetime(tu, tz.clone()),
        _ => unreachable!(),
    };

    let (mut start, mut stop) = match dtype {
        #[cfg(feature = "timezones")]
        DataType::Datetime(_, Some(_)) => (
            polars_ops::prelude::replace_time_zone(
                start.cast(&dtype)?.datetime().unwrap(),
                None,
                None,
            )?
            .into_series()
            .to_physical_repr()
            .cast(&DataType::Int64)?,
            polars_ops::prelude::replace_time_zone(
                stop.cast(&dtype)?.datetime().unwrap(),
                None,
                None,
            )?
            .into_series()
            .to_physical_repr()
            .cast(&DataType::Int64)?,
        ),
        _ => (
            start
                .cast(&dtype)?
                .to_physical_repr()
                .cast(&DataType::Int64)?,
            stop.cast(&dtype)?
                .to_physical_repr()
                .cast(&DataType::Int64)?,
        ),
    };

    if dtype == DataType::Date {
        start = &start * TO_MS;
        stop = &stop * TO_MS;
    }

    // overwrite time zone, if specified
    match (&dtype, &time_zone) {
        #[cfg(feature = "timezones")]
        (DataType::Datetime(tu, _), Some(tz)) => {
            dtype = DataType::Datetime(*tu, Some(tz.clone()));
        },
        _ => {},
    };

    let start = start.i64().unwrap();
    let stop = stop.i64().unwrap();

    let list = match dtype {
        DataType::Date => {
            let mut builder = ListPrimitiveChunkedBuilder::<Int32Type>::new(
                name,
                start.len(),
                start.len() * 5,
                DataType::Int32,
            );
            for (start, stop) in start.into_iter().zip(stop) {
                match (start, stop) {
                    (Some(start), Some(stop)) => {
                        let rng = date_range_impl(
                            "",
                            start,
                            stop,
                            every,
                            closed,
                            TimeUnit::Milliseconds,
                            None,
                        )?;
                        let rng = rng.cast(&DataType::Date).unwrap();
                        let rng = rng.to_physical_repr();
                        let rng = rng.i32().unwrap();
                        builder.append_slice(rng.cont_slice().unwrap())
                    },
                    _ => builder.append_null(),
                }
            }
            builder.finish().into_series()
        },
        DataType::Datetime(tu, ref tz) => {
            let mut builder = ListPrimitiveChunkedBuilder::<Int64Type>::new(
                name,
                start.len(),
                start.len() * 5,
                DataType::Int64,
            );
            for (start, stop) in start.into_iter().zip(stop) {
                match (start, stop) {
                    (Some(start), Some(stop)) => {
                        let rng = date_range_impl("", start, stop, every, closed, tu, tz.as_ref())?;
                        builder.append_slice(rng.cont_slice().unwrap())
                    },
                    _ => builder.append_null(),
                }
            }
            builder.finish().into_series()
        },
        DataType::Time => {
            let mut builder = ListPrimitiveChunkedBuilder::<Int64Type>::new(
                name,
                start.len(),
                start.len() * 5,
                DataType::Int64,
            );
            for (start, stop) in start.into_iter().zip(stop) {
                match (start, stop) {
                    (Some(start), Some(stop)) => {
                        let rng = date_range_impl(
                            "",
                            start,
                            stop,
                            every,
                            closed,
                            TimeUnit::Nanoseconds,
                            None,
                        )?;
                        builder.append_slice(rng.cont_slice().unwrap())
                    },
                    _ => builder.append_null(),
                }
            }
            builder.finish().into_series()
        },
        _ => unimplemented!(),
    };

    let to_type = DataType::List(Box::new(dtype));
    list.cast(&to_type)
}
