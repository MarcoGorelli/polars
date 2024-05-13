from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

import polars as pl
from polars.testing import assert_frame_equal, assert_series_equal

if TYPE_CHECKING:
    from polars.type_aliases import ClosedInterval, PolarsIntegerType


@pytest.mark.parametrize("dtype", [pl.UInt32, pl.UInt64, pl.Int32, pl.Int64])
def test_rolling_group_by_overlapping_groups(dtype: PolarsIntegerType) -> None:
    # this first aggregates overlapping groups so they cannot be naively flattened
    df = pl.DataFrame({"a": [41, 60, 37, 51, 52, 39, 40]})

    assert_series_equal(
        (
            df.with_row_index()
            .with_columns(pl.col("index").cast(dtype))
            .rolling(index_column="index", period="5i")
            .agg(
                # trigger the apply on the expression engine
                pl.col("a").map_elements(lambda x: x).sum()
            )
        )["a"],
        df["a"].rolling_sum(window_size=5, min_periods=1),
    )


@pytest.mark.parametrize("input", [[pl.col("b").sum()], pl.col("b").sum()])
@pytest.mark.parametrize("dtype", [pl.UInt32, pl.UInt64, pl.Int32, pl.Int64])
def test_rolling_agg_input_types(input: Any, dtype: PolarsIntegerType) -> None:
    df = pl.LazyFrame(
        {"index_column": [0, 1, 2, 3], "b": [1, 3, 1, 2]},
        schema_overrides={"index_column": dtype},
    ).set_sorted("index_column")
    result = df.rolling(index_column="index_column", period="2i").agg(input)
    expected = pl.LazyFrame(
        {"index_column": [0, 1, 2, 3], "b": [1, 4, 4, 3]},
        schema_overrides={"index_column": dtype},
    )
    assert_frame_equal(result, expected)


@pytest.mark.parametrize("input", [str, "b".join])
def test_rolling_agg_bad_input_types(input: Any) -> None:
    df = pl.LazyFrame({"index_column": [0, 1, 2, 3], "b": [1, 3, 1, 2]}).set_sorted(
        "index_column"
    )
    with pytest.raises(TypeError):
        df.rolling(index_column="index_column", period="2i").agg(input)


def test_rolling_negative_offset_3914() -> None:
    df = pl.DataFrame(
        {
            "datetime": pl.datetime_range(
                datetime(2020, 1, 1), datetime(2020, 1, 5), "1d", eager=True
            ),
        }
    )
    result = df.rolling(index_column="datetime", period="2d", offset="-4d").agg(
        pl.len()
    )
    assert result["len"].to_list() == [0, 0, 1, 2, 2]

    df = pl.DataFrame({"ints": range(20)})

    result = df.rolling(index_column="ints", period="2i", offset="-5i").agg(
        pl.col("ints").alias("matches")
    )
    expected = [
        [],
        [],
        [],
        [0],
        [0, 1],
        [1, 2],
        [2, 3],
        [3, 4],
        [4, 5],
        [5, 6],
        [6, 7],
        [7, 8],
        [8, 9],
        [9, 10],
        [10, 11],
        [11, 12],
        [12, 13],
        [13, 14],
        [14, 15],
        [15, 16],
    ]
    assert result["matches"].to_list() == expected


@pytest.mark.parametrize("time_zone", [None, "US/Central"])
def test_rolling_negative_offset_crossing_dst(time_zone: str | None) -> None:
    df = pl.DataFrame(
        {
            "datetime": pl.datetime_range(
                datetime(2021, 11, 6),
                datetime(2021, 11, 9),
                "1d",
                time_zone=time_zone,
                eager=True,
            ),
            "value": [1, 4, 9, 155],
        }
    )
    result = df.rolling(index_column="datetime", period="2d", offset="-1d").agg(
        pl.col("value")
    )
    expected = pl.DataFrame(
        {
            "datetime": pl.datetime_range(
                datetime(2021, 11, 6),
                datetime(2021, 11, 9),
                "1d",
                time_zone=time_zone,
                eager=True,
            ),
            "value": [[1, 4], [4, 9], [9, 155], [155]],
        }
    )
    assert_frame_equal(result, expected)


@pytest.mark.parametrize("time_zone", [None, "US/Central"])
@pytest.mark.parametrize(
    ("offset", "closed", "expected_values"),
    [
        ("0d", "left", [[1, 4], [4, 9], [9, 155], [155]]),
        ("0d", "right", [[4, 9], [9, 155], [155], []]),
        ("0d", "both", [[1, 4, 9], [4, 9, 155], [9, 155], [155]]),
        ("0d", "none", [[4], [9], [155], []]),
        ("1d", "left", [[4, 9], [9, 155], [155], []]),
        ("1d", "right", [[9, 155], [155], [], []]),
        ("1d", "both", [[4, 9, 155], [9, 155], [155], []]),
        ("1d", "none", [[9], [155], [], []]),
    ],
)
def test_rolling_non_negative_offset_9077(
    time_zone: str | None,
    offset: str,
    closed: ClosedInterval,
    expected_values: list[list[int]],
) -> None:
    df = pl.DataFrame(
        {
            "datetime": pl.datetime_range(
                datetime(2021, 11, 6),
                datetime(2021, 11, 9),
                "1d",
                time_zone=time_zone,
                eager=True,
            ),
            "value": [1, 4, 9, 155],
        }
    )
    result = df.rolling(
        index_column="datetime", period="2d", offset=offset, closed=closed
    ).agg(pl.col("value"))
    expected = pl.DataFrame(
        {
            "datetime": pl.datetime_range(
                datetime(2021, 11, 6),
                datetime(2021, 11, 9),
                "1d",
                time_zone=time_zone,
                eager=True,
            ),
            "value": expected_values,
        }
    )
    assert_frame_equal(result, expected)


def test_rolling_dynamic_sortedness_check() -> None:
    # when the by argument is passed, the sortedness flag
    # will be unset as the take shuffles data, so we must explicitly
    # check the sortedness
    df = pl.DataFrame(
        {
            "idx": [1, 2, -1, 2, 1, 1],
            "group": [1, 1, 1, 2, 2, 1],
        }
    )

    with pytest.raises(pl.ComputeError, match=r"input data is not sorted"):
        df.rolling("idx", period="2i", group_by="group").agg(
            pl.col("idx").alias("idx1")
        )

    # no `group_by` argument
    with pytest.raises(
        pl.InvalidOperationError,
        match="argument in operation 'rolling' is not explicitly sorted",
    ):
        df.rolling("idx", period="2i").agg(pl.col("idx").alias("idx1"))


def test_rolling_empty_groups_9973() -> None:
    dt1 = date(2001, 1, 1)
    dt2 = date(2001, 1, 2)

    data = pl.DataFrame(
        {
            "id": ["A", "A", "B", "B", "C", "C"],
            "date": [dt1, dt2, dt1, dt2, dt1, dt2],
            "value": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        }
    ).sort(by=["id", "date"])

    expected = pl.DataFrame(
        {
            "id": ["A", "A", "B", "B", "C", "C"],
            "date": [
                date(2001, 1, 1),
                date(2001, 1, 2),
                date(2001, 1, 1),
                date(2001, 1, 2),
                date(2001, 1, 1),
                date(2001, 1, 2),
            ],
            "value": [[2.0], [], [4.0], [], [6.0], []],
        }
    )

    out = data.rolling(
        index_column="date",
        group_by="id",
        period="2d",
        offset="1d",
        closed="left",
        check_sorted=True,
    ).agg(pl.col("value"))

    assert_frame_equal(out, expected)


def test_rolling_duplicates_11281() -> None:
    df = pl.DataFrame(
        {
            "ts": [
                datetime(2020, 1, 1),
                datetime(2020, 1, 2),
                datetime(2020, 1, 2),
                datetime(2020, 1, 2),
                datetime(2020, 1, 3),
                datetime(2020, 1, 4),
            ],
            "val": [1, 2, 2, 2, 3, 4],
        }
    ).sort("ts")
    result = df.rolling("ts", period="1d", closed="left").agg(pl.col("val"))
    expected = df.with_columns(val=pl.Series([[], [1], [1], [1], [2, 2, 2], [3]]))
    assert_frame_equal(result, expected)


def test_rolling_check_sorted_15225() -> None:
    df = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)],
            "c": [1, 1, 2],
        }
    )
    result = df.rolling("b", period="2d", check_sorted=False).agg(pl.sum("a"))
    expected = pl.DataFrame(
        {"b": [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)], "a": [1, 3, 5]}
    )
    assert_frame_equal(result, expected)
    result = df.rolling("b", period="2d", group_by="c", check_sorted=False).agg(
        pl.sum("a")
    )
    expected = pl.DataFrame(
        {
            "c": [1, 1, 2],
            "b": [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)],
            "a": [1, 3, 3],
        }
    )
    assert_frame_equal(result, expected)
    with pytest.raises(pl.InvalidOperationError, match="not explicitly sorted"):
        result = df.rolling("b", period="2d").agg(pl.sum("a"))


def test_multiple_rolling_in_single_expression() -> None:
    df = pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                datetime(2024, 1, 12),
                datetime(2024, 1, 12, 0, 0, 0, 150_000),
                "10ms",
                eager=True,
                closed="left",
            ),
            "price": [0] * 15,
        }
    )

    front_count = (
        pl.col("price")
        .count()
        .rolling("timestamp", period=timedelta(milliseconds=100))
        .cast(pl.Int64)
    )
    back_count = (
        pl.col("price")
        .count()
        .rolling("timestamp", period=timedelta(milliseconds=200))
        .cast(pl.Int64)
    )
    assert df.with_columns(
        back_count.alias("back"),
        front_count.alias("front"),
        (back_count - front_count).alias("back - front"),
    )["back - front"].to_list() == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5]


def test_negative_zero_offset_16168() -> None:
    df = pl.DataFrame({"foo": [1] * 3}).sort("foo").with_row_index()
    result = df.rolling(index_column="foo", period="1i", offset="0i").agg("index")
    expected = pl.DataFrame(
        {"foo": [1, 1, 1], "index": [[], [], []]},
        schema_overrides={"index": pl.List(pl.UInt32)},
    )
    assert_frame_equal(result, expected)
    result = df.rolling(index_column="foo", period="1i", offset="-0i").agg("index")
    assert_frame_equal(result, expected)
