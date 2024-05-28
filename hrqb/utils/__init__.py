"""hrqb.utils"""

import datetime

import click
import pandas as pd
import us  # type: ignore[import-untyped]
from dateutil.parser import parse as date_parser  # type: ignore[import-untyped]


def today_date() -> datetime.date:
    return datetime.datetime.now(tz=datetime.UTC).date()


def normalize_date(date: str | datetime.datetime) -> str | None:
    if date is None or date == "" or pd.isna(date):
        return None
    if isinstance(date, str):
        date = date_parser(date)
    if isinstance(date, datetime.datetime):
        return date.strftime("%Y-%m-%d")
    return None


def normalize_dataframe_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df[columns] = df[columns].map(normalize_date)
    return df


def click_argument_to_dict(
    _ctx: click.Context, _parameter: click.Parameter, value: str
) -> dict:
    if value is None:
        return {}
    return dict(pair.split("=") for pair in value.split(","))


def convert_oracle_bools_to_qb_bools(
    dataframe: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """Convert Oracle bools [Y,N] to Quickbase bools [Yes,No] for a subset of columns."""
    dataframe[columns] = dataframe[columns].replace({"Y": "Yes", "N": "No"})
    return dataframe


def state_abbreviation_to_name(abbreviation: str | None) -> str | None:
    try:
        state = getattr(us.states, abbreviation or "None")
    except AttributeError:
        state = None
    if state:
        return state.name
    return None
