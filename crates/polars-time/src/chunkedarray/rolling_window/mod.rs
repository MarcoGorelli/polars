mod floats;
mod ints;
#[cfg(feature = "rolling_window")]
mod rolling_kernels;

#[cfg(feature = "rolling_window")]
use std::convert::TryFrom;
use std::ops::SubAssign;

#[cfg(feature = "rolling_window")]
use arrow::array::{Array, PrimitiveArray};
use polars_arrow::data_types::IsFloat;
#[cfg(feature = "rolling_window")]
use polars_arrow::export::arrow;
#[cfg(feature = "rolling_window")]
use polars_arrow::kernels::rolling;
use polars_core::prelude::*;

#[cfg(feature = "rolling_window")]
use crate::prelude::*;
use crate::series::WrapFloat;

#[derive(Clone)]
#[cfg(feature = "rolling_window")]
pub struct RollingOptions<'a> {
    /// The length of the window.
    pub window_size: Duration<'a>,
    /// Amount of elements in the window that should be filled before computing a result.
    pub min_periods: usize,
    /// An optional slice with the same length as the window that will be multiplied
    ///              elementwise with the values in the window.
    pub weights: Option<Vec<f64>>,
    /// Set the labels at the center of the window.
    pub center: bool,
    /// Compute the rolling aggregates with a window defined by a time column
    pub by: Option<String>,
    /// The closed window of that time window if given
    pub closed_window: Option<ClosedWindow>,
    /// Optional parameters for the rolling function
    pub fn_params: DynArgs,
}

#[cfg(feature = "rolling_window")]
impl <'a> Default for RollingOptions<'a> {
    fn default() -> Self {
        RollingOptions {
            window_size: Duration::parse("3i"),
            min_periods: 1,
            weights: None,
            center: false,
            by: None,
            closed_window: None,
            fn_params: None,
        }
    }
}

#[derive(Clone)]
#[cfg(feature = "rolling_window")]
pub struct RollingOptionsImpl<'a> {
    /// The length of the window.
    pub window_size: Duration<'a>,
    /// Amount of elements in the window that should be filled before computing a result.
    pub min_periods: usize,
    /// An optional slice with the same length as the window that will be multiplied
    ///              elementwise with the values in the window.
    pub weights: Option<Vec<f64>>,
    /// Set the labels at the center of the window.
    pub center: bool,
    pub by: Option<&'a [i64]>,
    pub tu: Option<TimeUnit>,
    pub tz: Option<&'a TimeZone>,
    pub closed_window: Option<ClosedWindow>,
    pub fn_params: DynArgs,
}

#[cfg(feature = "rolling_window")]
impl From<RollingOptions<'_>> for RollingOptionsImpl<'static> {
    fn from(options: RollingOptions) -> Self {
        let window_size = options.window_size;
        assert!(
            window_size.parsed_int,
            "should be fixed integer window size at this point"
        );

        RollingOptionsImpl {
            window_size,
            min_periods: options.min_periods,
            weights: options.weights,
            center: options.center,
            by: None,
            tu: None,
            tz: None,
            closed_window: None,
            fn_params: options.fn_params,
        }
    }
}

#[cfg(feature = "rolling_window")]
impl From<RollingOptions<'_>> for RollingOptionsFixedWindow {
    fn from(options: RollingOptions) -> Self {
        let window_size = options.window_size;
        assert!(
            window_size.parsed_int,
            "should be fixed integer window size at this point"
        );

        RollingOptionsFixedWindow {
            window_size: window_size.nanoseconds() as usize,
            min_periods: options.min_periods,
            weights: options.weights,
            center: options.center,
            fn_params: options.fn_params,
        }
    }
}

#[cfg(feature = "rolling_window")]
impl Default for RollingOptionsImpl<'static> {
    fn default() -> Self {
        RollingOptionsImpl {
            window_size: Duration::parse("3i"),
            min_periods: 1,
            weights: None,
            center: false,
            by: None,
            tu: None,
            tz: None,
            closed_window: None,
            fn_params: None,
        }
    }
}

#[cfg(feature = "rolling_window")]
impl<'a> From<RollingOptionsImpl<'a>> for RollingOptionsFixedWindow {
    fn from(options: RollingOptionsImpl<'a>) -> Self {
        let window_size = options.window_size;
        assert!(
            window_size.parsed_int,
            "should be fixed integer window size at this point"
        );

        RollingOptionsFixedWindow {
            window_size: window_size.nanoseconds() as usize,
            min_periods: options.min_periods,
            weights: options.weights,
            center: options.center,
            fn_params: options.fn_params,
        }
    }
}

#[cfg(not(feature = "rolling_window"))]
pub trait RollingAgg {}

#[cfg(feature = "rolling_window")]
pub trait RollingAgg {
    /// Apply a rolling mean (moving mean) over the values in this array.
    /// A window of length `window_size` will traverse the array. The values that fill this window
    /// will (optionally) be multiplied with the weights given by the `weights` vector. The resulting
    /// values will be aggregated to their mean.
    fn rolling_mean(&self, options: RollingOptionsImpl) -> PolarsResult<Series>;

    /// Apply a rolling sum (moving sum) over the values in this array.
    /// A window of length `window_size` will traverse the array. The values that fill this window
    /// will (optionally) be multiplied with the weights given by the `weights` vector. The resulting
    /// values will be aggregated to their sum.
    fn rolling_sum(&self, options: RollingOptionsImpl) -> PolarsResult<Series>;

    /// Apply a rolling min (moving min) over the values in this array.
    /// A window of length `window_size` will traverse the array. The values that fill this window
    /// will (optionally) be multiplied with the weights given by the `weights` vector. The resulting
    /// values will be aggregated to their min.
    fn rolling_min(&self, options: RollingOptionsImpl) -> PolarsResult<Series>;

    /// Apply a rolling max (moving max) over the values in this array.
    /// A window of length `window_size` will traverse the array. The values that fill this window
    /// will (optionally) be multiplied with the weights given by the `weights` vector. The resulting
    /// values will be aggregated to their max.
    fn rolling_max(&self, options: RollingOptionsImpl) -> PolarsResult<Series>;

    /// Apply a rolling median (moving median) over the values in this array.
    /// A window of length `window_size` will traverse the array. The values that fill this window
    /// will (optionally) be weighted according to the `weights` vector.
    fn rolling_median(&self, options: RollingOptionsImpl) -> PolarsResult<Series>;

    /// Apply a rolling quantile (moving quantile) over the values in this array.
    /// A window of length `window_size` will traverse the array. The values that fill this window
    /// will (optionally) be weighted according to the `weights` vector.
    fn rolling_quantile(&self, options: RollingOptionsImpl) -> PolarsResult<Series>;

    /// Apply a rolling var (moving var) over the values in this array.
    /// A window of length `window_size` will traverse the array. The values that fill this window
    /// will (optionally) be multiplied with the weights given by the `weights` vector. The resulting
    /// values will be aggregated to their var.
    #[cfg(feature = "rolling_window")]
    fn rolling_var(&self, options: RollingOptionsImpl) -> PolarsResult<Series>;

    /// Apply a rolling std (moving std) over the values in this array.
    /// A window of length `window_size` will traverse the array. The values that fill this window
    /// will (optionally) be multiplied with the weights given by the `weights` vector. The resulting
    /// values will be aggregated to their std.
    fn rolling_std(&self, options: RollingOptionsImpl) -> PolarsResult<Series>;
}

/// utility
#[cfg(feature = "rolling_window")]
fn check_input(window_size: usize, min_periods: usize) -> PolarsResult<()> {
    polars_ensure!(
        min_periods <= window_size,
        ComputeError: "`min_periods` should be <= `window_size`",
    );
    Ok(())
}

#[cfg(feature = "rolling_window")]
#[allow(clippy::type_complexity)]
fn rolling_agg<T>(
    ca: &ChunkedArray<T>,
    options: RollingOptionsImpl,
    rolling_agg_fn: &dyn Fn(
        &[T::Native],
        usize,
        usize,
        bool,
        Option<&[f64]>,
        DynArgs,
    ) -> PolarsResult<ArrayRef>,
    rolling_agg_fn_nulls: &dyn Fn(
        &PrimitiveArray<T::Native>,
        usize,
        usize,
        bool,
        Option<&[f64]>,
        DynArgs,
    ) -> ArrayRef,
    rolling_agg_fn_dynamic: Option<
        &dyn Fn(
            &[T::Native],
            Duration,
            &[i64],
            ClosedWindow,
            TimeUnit,
            Option<&TimeZone>,
            DynArgs,
        ) -> PolarsResult<ArrayRef>,
    >,
) -> PolarsResult<Series>
where
    T: PolarsNumericType,
{
    if ca.is_empty() {
        return Ok(Series::new_empty(ca.name(), ca.dtype()));
    }
    let ca = ca.rechunk();

    let arr = ca.downcast_iter().next().unwrap();
    // "5i" is a window size of 5, e.g. fixed
    let arr = if options.window_size.parsed_int {
        let options: RollingOptionsFixedWindow = options.into();
        check_input(options.window_size, options.min_periods)?;

        Ok(match ca.null_count() {
            0 => rolling_agg_fn(
                arr.values().as_slice(),
                options.window_size,
                options.min_periods,
                options.center,
                options.weights.as_deref(),
                options.fn_params,
            )?,
            _ => rolling_agg_fn_nulls(
                arr,
                options.window_size,
                options.min_periods,
                options.center,
                options.weights.as_deref(),
                options.fn_params,
            ),
        })
    } else {
        if arr.null_count() > 0 {
            panic!("'rolling by' not yet supported for series with null values, consider using 'groupby_rolling'")
        }
        let values = arr.values().as_slice();
        let duration = options.window_size;
        polars_ensure!(duration.duration_ns() > 0 && !duration.negative, ComputeError:"window size should be strictly positive");
        let tu = options.tu.unwrap();
        let by = options.by.unwrap();
        let closed_window = options.closed_window.expect("closed window  must be set");
        let func = rolling_agg_fn_dynamic.expect(
            "'rolling by' not yet supported for this expression, consider using 'groupby_rolling'",
        );

        func(
            values,
            duration,
            by,
            closed_window,
            tu,
            options.tz,
            options.fn_params,
        )
    }?;
    Series::try_from((ca.name(), arr))
}
