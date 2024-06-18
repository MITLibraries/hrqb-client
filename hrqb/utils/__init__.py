"""hrqb.utils"""

import datetime
import hashlib
import logging

import click
import pandas as pd
import us  # type: ignore[import-untyped]
from dateutil.parser import ParserError  # type: ignore[import-untyped]
from dateutil.parser import parse as date_parser  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


def today_date() -> datetime.date:
    return datetime.datetime.now(tz=datetime.UTC).date()


def normalize_date(date: str | datetime.datetime) -> str | None:
    if date == "" or pd.isna(date):
        return None
    if isinstance(date, str):
        try:
            date = date_parser(date)
        except ParserError:
            message = f"Unable to parse date from '{date}'"
            logger.warning(message)
    if isinstance(date, datetime.datetime):
        return date.strftime("%Y-%m-%d")
    return None


def convert_dataframe_columns_to_dates(
    df: pd.DataFrame, columns: list[str]
) -> pd.DataFrame:
    """Convert select columns from a dataframe to datetime objects.

    This more manual approach avoids a pandas error with pd.to_datetime() when the date
    exceeds 2262-04-11.  Normally this would not be a problem, but employee appointments
    that are ongoing receive a datetime of 2999-12-31. See: https://pandas.pydata.org/
    pandas-docs/stable/user_guide/timeseries.html#timestamp-limitations.
    """

    def convert_to_date(
        value: str | datetime.datetime | pd.Timestamp,
    ) -> datetime.datetime | pd.Timestamp | None:
        if isinstance(value, str):
            return date_parser(value).replace(tzinfo=datetime.UTC)
        if isinstance(value, datetime.datetime | pd.Timestamp):
            return value.replace(tzinfo=datetime.UTC)
        return None

    for column in columns:
        df[column] = df[column].apply(lambda x: convert_to_date(x))
    return df


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


def us_state_abbreviation_to_name(abbreviation: str | None) -> str | None:
    try:
        state = getattr(us.states, abbreviation or "None")
    except AttributeError:
        state = None
    if state:
        return state.name
    return None


def md5_hash_from_values(values: list[str]) -> str:
    """Create an MD5 hash from an ordered list of values.

    This function is used by some Transform tasks to mint a deterministic, unique value
    that can be used as merge fields in Quickbase during upserts.

    NOTE: the order of values in the provided list will affect the MD5 hash created.
    """
    data_string = "|".join(values).encode()
    return hashlib.md5(data_string).hexdigest()  # noqa: S324
