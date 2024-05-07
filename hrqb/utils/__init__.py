"""hrqb.utils"""

import datetime

import click


def today_date() -> datetime.date:
    return datetime.datetime.now(tz=datetime.UTC).date()


def dynamic_import(module_name: str, class_name: str) -> object:
    module = __import__(module_name, fromlist=[class_name])
    return getattr(module, class_name)


def click_argument_to_dict(
    _ctx: click.Context, _parameter: click.Parameter, value: str
) -> dict:
    if value is None:
        return {}
    return dict(pair.split("=") for pair in value.split(","))
