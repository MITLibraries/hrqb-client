# ruff: noqa: DTZ005, PD901, DTZ001

import datetime

import numpy as np
import pandas as pd
import pytest
from dateutil.parser import ParserError  # type: ignore[import-untyped]
from freezegun import freeze_time

from hrqb.utils import (
    click_argument_to_dict,
    convert_dataframe_columns_to_dates,
    convert_oracle_bools_to_qb_bools,
    md5_hash_from_values,
    normalize_dataframe_dates,
    normalize_date,
    today_date,
    us_state_abbreviation_to_name,
)


@freeze_time("2000-01-01 01:23:45", tz_offset=0)
def test_today_date():
    assert today_date() == datetime.date(2000, 1, 1)


def test_click_argument_to_dict_empty_value_return_empty_dict():
    assert click_argument_to_dict(None, None, None) == {}


def test_click_argument_to_dict_single_param():
    assert click_argument_to_dict(None, None, "ParamA=foo") == {"ParamA": "foo"}


def test_click_argument_to_dict_multiple_params():
    assert click_argument_to_dict(None, None, "ParamA=foo,ParamB=bar") == {
        "ParamA": "foo",
        "ParamB": "bar",
    }


@freeze_time("2000-01-01 01:23:45", tz_offset=0)
def test_normalize_date_date_object_or_string_return_yyyy_mm_dd():
    assert normalize_date(datetime.datetime.now()) == "2000-01-01"
    assert normalize_date("2000-01-01 01:23:45") == "2000-01-01"


def test_normalize_date_unparsable_values_return_none(caplog):
    assert normalize_date("") is None
    assert normalize_date(None) is None
    assert normalize_date(np.nan) is None
    assert normalize_date(42) is None
    assert normalize_date("I cannot be parsed.") is None
    assert "Unable to parse date from 'I cannot be parsed.'" in caplog.text


def test_normalize_dataframe_dates_success():
    df = pd.DataFrame(
        [
            {
                "x": "2000-01-01 01:23:45",
                "y": datetime.datetime(2000, 1, 1),
                "q": np.nan,
                "z": "horse",
            },
            {
                "x": "2010-12-31 01:23:45",
                "y": datetime.datetime(2010, 12, 31),
                "q": np.nan,
                "z": "zebra",
            },
        ]
    )
    df = normalize_dataframe_dates(df, ["x", "y", "q"])
    assert df.equals(
        pd.DataFrame(
            [
                {
                    "x": "2000-01-01",
                    "y": "2000-01-01",
                    "q": None,
                    "z": "horse",
                },
                {
                    "x": "2010-12-31",
                    "y": "2010-12-31",
                    "q": None,
                    "z": "zebra",
                },
            ]
        )
    )


def test_us_state_abbreviation_to_name_success():
    assert us_state_abbreviation_to_name("WA") == "Washington"


def test_us_state_abbreviation_to_name_missing_abbrevation_return_none():
    assert us_state_abbreviation_to_name("BAD") is None


def test_convert_oracle_bools_to_qb_bools_success():
    df = pd.DataFrame(
        [
            {"x": "Y", "y": 42},
            {"x": "N", "y": 42},
        ]
    )
    new_df = convert_oracle_bools_to_qb_bools(df, columns=["x"])
    assert new_df.equals(
        pd.DataFrame(
            [
                {"x": "Yes", "y": 42},
                {"x": "No", "y": 42},
            ]
        )
    )


def test_md5_hash_from_values_gives_expected_results():
    assert md5_hash_from_values(["a", "b", "c"]) == "2e077b3ec5932ac3cf914ebdf242b4ee"


def test_md5_hash_from_values_order_of_strings_different_results():
    assert md5_hash_from_values(["a", "b"]) != md5_hash_from_values(["b", "a"])


def test_md5_hash_from_values_raise_error_for_non_string_value():
    with pytest.raises(TypeError, match="NoneType found"):
        md5_hash_from_values(["a", "b", None])
    with pytest.raises(TypeError, match="int found"):
        md5_hash_from_values(["a", "b", 42])
    with pytest.raises(TypeError, match="datetime found"):
        md5_hash_from_values(
            [
                "a",
                "b",
                datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC),
            ]
        )


def test_convert_dataframe_columns_to_dates_conversion_success():
    df = pd.DataFrame(
        [
            {
                "foo": "2000-01-01",
                "bar": "horse",
            },
            {
                "foo": datetime.datetime(2020, 6, 1, tzinfo=datetime.UTC),
                "bar": "zebra",
            },
            {
                "foo": 42,
                "bar": "giraffe",
            },
        ]
    )
    assert convert_dataframe_columns_to_dates(df, ["foo"]).equals(
        pd.DataFrame(
            [
                {
                    "foo": datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC),
                    "bar": "horse",
                },
                {
                    "foo": datetime.datetime(2020, 6, 1, tzinfo=datetime.UTC),
                    "bar": "zebra",
                },
                {
                    "foo": None,
                    "bar": "giraffe",
                },
            ]
        )
    )


def test_convert_dataframe_columns_to_dates_bad_date_raise_error():
    df = pd.DataFrame([{"foo": "I CANNOT BE PARSED", "bar": "horse"}])
    with pytest.raises(ParserError, match="Unknown string format: I CANNOT BE PARSED"):
        assert convert_dataframe_columns_to_dates(df, ["foo"])
