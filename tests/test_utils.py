import datetime

from freezegun import freeze_time

from hrqb.utils import click_argument_to_dict, today_date


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
