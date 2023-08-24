use polars_arrow::kernels::replace_time_zone as replace_time_zone_kernel;
use polars_core::prelude::*;

pub fn replace_time_zone(
    ca: &DatetimeChunked,
    time_zone: Option<&str>,
    use_earliest: Option<bool>,
) -> PolarsResult<DatetimeChunked> {
    let out: PolarsResult<_> = {
        let from = ca.time_zone().as_deref().unwrap_or("UTC");
        let to = time_zone.unwrap_or("UTC");
        let (from_tz, to_tz) = match from.parse::<chrono_tz::Tz>() {
            Ok(from_tz) => match to.parse::<chrono_tz::Tz>() {
                Ok(to_tz) => (from_tz, to_tz),
                Err(_) => polars_bail!(ComputeError: "unable to parse time zone: '{}'", to),
            },
            Err(_) => polars_bail!(ComputeError: "unable to parse time zone: '{}'", from),
        };
        let chunks = ca.downcast_iter().map(|arr| {
            replace_time_zone_kernel(
                arr,
                ca.time_unit().to_arrow(),
                &from_tz,
                &to_tz,
                use_earliest,
            )
        });
        let out = ChunkedArray::try_from_chunk_iter(ca.name(), chunks)?;
        Ok(out.into_datetime(ca.time_unit(), time_zone.map(|x| x.to_string())))
    };
    let mut out = out?;
    out.set_sorted_flag(ca.is_sorted_flag());
    Ok(out)
}
