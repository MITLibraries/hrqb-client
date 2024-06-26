"""hrqb.base.target"""

import json

import luigi  # type: ignore[import-untyped]
import pandas as pd
from luigi.format import MixedUnicodeBytes  # type: ignore[import-untyped]


class HRQBLocalTarget(luigi.LocalTarget):
    """Target is local file with path and table name init."""

    def __init__(self, path: str, table_name: str) -> None:
        super().__init__(path, format=MixedUnicodeBytes)
        self.path = path
        self.table_name = table_name


class PandasPickleTarget(HRQBLocalTarget):
    """Target is pandas DataFrame pickled to disk."""

    def read(self) -> pd.DataFrame:
        return pd.read_pickle(self.path)

    def write(self, df: pd.DataFrame) -> None:
        df.to_pickle(self.path)

    @property
    def records_count(self) -> int | None:
        if self.exists():
            return len(self.read())
        return None


class QuickbaseTableTarget(HRQBLocalTarget):
    """Target is upsert to Quickbase table."""

    def read(self) -> dict:
        with open(self.path) as f:
            return json.load(f)

    def write(self, data: dict, indent: bool = True) -> int:  # noqa: FBT001, FBT002
        with open(self.path, "w") as f:
            return f.write(json.dumps(data, indent=indent))

    @property
    def records_count(self) -> int | None:
        if self.exists():
            return self.read()["metadata"]["totalNumberOfRecordsProcessed"]
        return None
