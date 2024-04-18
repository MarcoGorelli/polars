use arrow::legacy::time_zone::Tz;
use arrow::temporal_conversions::{MILLISECONDS, SECONDS_IN_DAY};
use polars_core::prelude::*;

use crate::prelude::*;

pub trait PolarsRound {
    fn round(&self, every: Duration, offset: Duration, tz: Option<&Tz>) -> PolarsResult<Self>
    where
        Self: Sized;
}

impl PolarsRound for DatetimeChunked {
    fn round(&self, every: Duration, offset: Duration, tz: Option<&Tz>) -> PolarsResult<Self> {
        if every.negative {
            polars_bail!(ComputeError: "cannot round a Datetime to a negative duration")
        }

        let w = Window::new(every, every, offset);

        let func = match self.time_unit() {
            TimeUnit::Nanoseconds => Window::round_ns,
            TimeUnit::Microseconds => Window::round_us,
            TimeUnit::Milliseconds => Window::round_ms,
        };

        let out = { self.try_apply_nonnull_values_generic(|t| func(&w, t, tz)) };
        out.map(|ok| ok.into_datetime(self.time_unit(), self.time_zone().clone()))
    }
}

impl PolarsRound for DateChunked {
    fn round(&self, every: Duration, offset: Duration, _tz: Option<&Tz>) -> PolarsResult<Self> {
        if every.negative {
            polars_bail!(ComputeError: "cannot round a Date to a negative duration")
        }

        let w = Window::new(every, every, offset);
        Ok(self
            .try_apply_nonnull_values_generic(|t| {
                const MSECS_IN_DAY: i64 = MILLISECONDS * SECONDS_IN_DAY;
                PolarsResult::Ok((w.round_ms(MSECS_IN_DAY * t as i64, None)? / MSECS_IN_DAY) as i32)
            })?
            .into_date())
    }
}

#[cfg(feature = "dtype-duration")]
impl PolarsRound for DurationChunked {
    fn round(&self, every: Duration, offset: Duration, tz: Option<&Tz>) -> PolarsResult<Self> {
        polars_ensure!(!every.negative, ComputeError: "cannot round a Duration to a negative duration");
        let time_zone = match tz {
            #[cfg(feature = "timezones")]
            Some(tz) => Some(tz.name()),
            _ => None,
        };
        ensure_is_constant_duration(every, time_zone, "every")?;
        polars_ensure!(offset.is_zero(), InvalidOperation: "`offset` is not supported for rounding Durations.");
        let every = match self.time_unit() {
            TimeUnit::Nanoseconds => every.duration_ns(),
            TimeUnit::Microseconds => every.duration_us(),
            TimeUnit::Milliseconds => every.duration_ms(),
        };
        polars_ensure!(
            every != 0,
            InvalidOperation: "duration cannot be zero."
        );

        let out = self.apply_values(|duration| {
            // Round half-way values away from zero
            let half_away = duration.signum() * every / 2;
            duration + half_away - (duration + half_away) % every
        });

        Ok(out.into_duration(self.time_unit()))
    }
}
