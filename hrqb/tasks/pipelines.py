"""hrqb.tasks.pipelines"""

from collections.abc import Iterator

import luigi  # type: ignore[import-untyped]

from hrqb.base.task import HRQBPipelineTask


class FullUpdate(HRQBPipelineTask):
    """Pipeline to perform a full update of all Quickbase tables."""

    def requires(self) -> Iterator[luigi.Task]:  # pragma: no cover
        from hrqb.tasks.employee_appointments import LoadEmployeeAppointments
        from hrqb.tasks.employee_leave import LoadEmployeeLeave
        from hrqb.tasks.employee_salary_history import LoadEmployeeSalaryHistory
        from hrqb.tasks.employees import LoadEmployees

        yield LoadEmployees(pipeline=self.pipeline_name)
        yield LoadEmployeeAppointments(pipeline=self.pipeline_name)
        yield LoadEmployeeSalaryHistory(pipeline=self.pipeline_name)
        yield LoadEmployeeLeave(pipeline=self.pipeline_name)


class UpdateLibHRData(HRQBPipelineTask):
    """Pipeline to load Library HR employee appointment data from static CSV file.

    This pipeline loads the table 'LibHR Employee Appointments', which contains
    information known only by Library HR, that we cannot get from the data warehouse,
    including:
        - position HC ID (Headcount ID)
        - position supervisor
        - position library department
        - position cost object

    This Quickbase table is used by the 'Employee Appointments' table to fill in gaps from
    warehouse data alone.  This pipeline is useful for initial loading and bulk changes,
    but this table is primarily managed directly in Quickbase by HR staff.

    This pipeline requires a 'csv_filepath' parameter is defined when running, e.g.
        pipenv run hrqb --verbose /
        pipeline -p UpdateLibHRData /
        --pipeline-parameters=csv_filepath=<PATH TO CSV> /
        run
    """

    csv_filepath = luigi.Parameter()

    def requires(self) -> Iterator[luigi.Task]:  # pragma: no cover
        from hrqb.tasks.libhr_employee_appointments import LoadLibHREmployeeAppointments

        yield LoadLibHREmployeeAppointments(
            pipeline=self.pipeline_name,
            csv_filepath=self.csv_filepath,
        )
