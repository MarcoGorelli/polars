use super::*;

macro_rules! prepare_binary_function {
    ($f:ident) => {
        move |s: &mut [Series]| {
            let s0 = std::mem::take(&mut s[0]);
            let s1 = std::mem::take(&mut s[1]);

            $f(s0, s1)
        }
    };
}

/// See [`Expr::apply`] for the difference between [`map`](Expr::map) and [`apply`](Expr::apply).
pub fn apply_binary<F: 'static>(a: Expr, b: Expr, f: F, output_type: GetOutput) -> Expr
where
    F: Fn(Series, Series) -> PolarsResult<Option<Series>> + Send + Sync,
{
    let function = prepare_binary_function!(f);
    a.apply_many(function, &[b], output_type)
}
