"""hrqb.tasks.years"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask
from hrqb.utils import today_date


class TransformYears(PandasPickleTask):
    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        from hrqb.tasks.performance_reviews import TransformPerformanceReviews

        return [TransformPerformanceReviews(pipeline=self.pipeline)]

    def get_dataframe(self) -> pd.DataFrame:
        perf_revs_df = self.single_input_dataframe
        perf_revs_df = perf_revs_df.rename(columns={"Related Year": "year"})

        years_df = perf_revs_df.drop_duplicates("year").copy()
        years_df["year"] = years_df["year"].astype(int)
        years_df["active"] = years_df["year"] == today_date().year
        years_df["year"] = years_df["year"].astype(str)

        fields = {
            "year": "Year",
            "active": "Active Year",
        }
        return years_df[fields.keys()].rename(columns=fields)


class LoadYears(QuickbaseUpsertTask):
    table_name = luigi.Parameter("Years")
    stage = luigi.Parameter("Load")

    @property
    def merge_field(self) -> str | None:
        return "Year"  # pragma: nocover

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [TransformYears(pipeline=self.pipeline)]
