from polars.type_aliases import IntoExpr
from typing import Sequence
import polars as pl
from datetime import date
import contextlib

from polars._utils.parse_expr_input import parse_as_expression
from polars._utils.wrap import wrap_expr

with contextlib.suppress(ImportError, NameError):
    # note: 'plr' not available when building docs
    import polars.polars as plr

mapping = {"Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6, "Sun": 7}
reverse_mapping = {value: key for key, value in mapping.items()}

def get_weekmask(weekend: Sequence[str]) -> list[bool]:
    if weekend == ("Sat", "Sun"):
        weekmask = [True, True, True, True, True, False, False]
    else:
        weekmask = [reverse_mapping[i] not in weekend for i in range(1, 8)]
    if sum(weekmask) == 0:
        msg = f"At least one day of the week must be a business day. Got weekend={weekend}"
        raise ValueError(msg)
    return weekmask


def business_day_count(
    start_dates: IntoExpr,
    end_dates: IntoExpr,
) -> pl.Expr:
    """
    Count the number of workdays between two columns of dates.

    Parameters
    ----------
    start_dates
        Start date(s). This can be a string column, a date column, or a single date.
    end_dates
        End date(s). This can be a string column, a date column, or a single date.

    Returns
    -------
    polars.Expr

    Examples
    --------
    >>> from datetime import date
    >>> import polars as pl
    >>> df = pl.DataFrame(
    ...     {
    ...         "start": [date(2023, 1, 4), date(2023, 5, 1), date(2023, 9, 9)],
    ...         "end": [date(2023, 2, 8), date(2023, 5, 2), date(2023, 12, 30)],
    ...     }
    ... )
    >>> df.with_columns(n_business_days=pl.business_day_count("start", "end"))
    shape: (3, 3)
    ┌────────────┬────────────┬─────────────────┐
    │ start      ┆ end        ┆ n_business_days │
    │ ---        ┆ ---        ┆ ---             │
    │ date       ┆ date       ┆ i32             │
    ╞════════════╪════════════╪═════════════════╡
    │ 2023-01-04 ┆ 2023-02-08 ┆ 25              │
    │ 2023-05-01 ┆ 2023-05-02 ┆ 1               │
    │ 2023-09-09 ┆ 2023-12-30 ┆ 80              │
    └────────────┴────────────┴─────────────────┘

    """
    start_dates_pyexpr = parse_as_expression(start_dates)
    end_dates_pyexpr = parse_as_expression(end_dates)
    return wrap_expr(plr.business_day_count(start_dates_pyexpr, end_dates_pyexpr))
