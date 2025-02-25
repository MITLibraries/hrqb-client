"""hrqb.tasks.pipelines"""

from collections.abc import Iterator

import luigi  # type: ignore[import-untyped]

from hrqb.base.task import HRQBPipelineTask


class FullUpdate(HRQBPipelineTask):
    """Pipeline to perform a full update of all Quickbase tables."""

    def requires(self) -> Iterator[luigi.Task]:  # pragma: no cover
        from hrqb.tasks.employee_appointments import LoadEmployeeAppointments
        from hrqb.tasks.employee_leave import LoadEmployeeLeave
        from hrqb.tasks.employee_leave_balances import LoadEmployeeLeaveBalances
        from hrqb.tasks.employee_salary_history import LoadEmployeeSalaryHistory
        from hrqb.tasks.employees import LoadEmployees
        from hrqb.tasks.performance_reviews import LoadPerformanceReviews

        yield LoadEmployees(pipeline=self.pipeline_name)
        yield LoadEmployeeAppointments(pipeline=self.pipeline_name)
        yield LoadEmployeeSalaryHistory(pipeline=self.pipeline_name)
        yield LoadEmployeeLeave(pipeline=self.pipeline_name)
        yield LoadPerformanceReviews(pipeline=self.pipeline_name)
        yield LoadEmployeeLeaveBalances(pipeline=self.pipeline_name)
