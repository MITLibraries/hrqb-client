"""hrqb.utils"""

import datetime


def today_date() -> datetime.date:
    return datetime.datetime.now(tz=datetime.UTC).date()
