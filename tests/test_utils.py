# ruff: noqa: DTZ005, PD901, DTZ001

import datetime

import numpy as np
import pandas as pd
from freezegun import freeze_time

from hrqb.utils import (
    click_argument_to_dict,
    normalize_dataframe_dates,
    normalize_date,
    today_date,
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


def test_normalize_date_unparsable_values_return_none():
    assert normalize_date("") is None
    assert normalize_date(None) is None
    assert normalize_date(np.nan) is None
    assert normalize_date(42) is None


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
