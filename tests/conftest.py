# ruff: noqa: N802, N803, DTZ001, PLR2004, TRY003, EM101

import datetime
import json
import shutil
from unittest import mock

import luigi
import numpy as np
import pandas as pd
import pytest
import requests_mock
from click.testing import CliRunner
from pandas import Timestamp

from hrqb.base import HRQBTask, QuickbaseTableTarget
from hrqb.base.task import PandasPickleTarget, PandasPickleTask, QuickbaseUpsertTask
from hrqb.utils.data_warehouse import DWClient
from hrqb.utils.quickbase import QBClient
from tests.fixtures.tasks.extract import (
    ExtractAnimalColors,
    ExtractAnimalNames,
    SQLExtractAnimalColors,
    SQLExtractAnimalNames,
    SQLQueryWithParameters,
)
from tests.fixtures.tasks.load import LoadAnimals
from tests.fixtures.tasks.pipelines import Animals, AnimalsDebug, Creatures
from tests.fixtures.tasks.transform import PrepareAnimals


@pytest.fixture(autouse=True)
def _test_env(request, monkeypatch, targets_directory, data_warehouse_connection_string):
    if request.node.get_closest_marker("integration"):
        return
    monkeypatch.setenv("SENTRY_DSN", "None")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("LUIGI_CONFIG_PATH", "hrqb/luigi.cfg")
    monkeypatch.setenv("QUICKBASE_API_URL", "http://qb.example.org/v1")
    monkeypatch.setenv("QUICKBASE_API_TOKEN", "qb-api-acb123")
    monkeypatch.setenv("QUICKBASE_APP_ID", "qb-app-def456")
    monkeypatch.setenv("TARGETS_DIRECTORY", str(targets_directory))
    monkeypatch.setenv(
        "DATA_WAREHOUSE_CONNECTION_STRING",
        data_warehouse_connection_string,
    )
    monkeypatch.setenv("SKIP_TASK_INTEGRITY_CHECKS", "1")


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def targets_directory(tmp_path_factory):
    return tmp_path_factory.mktemp("targets")


@pytest.fixture
def generic_hrqb_task_class():
    class GenericTask(HRQBTask):
        @property
        def target(self):
            return luigi.LocalTarget(path=self.path)

        @property
        def filename_extension(self):
            return ".csv"

    return GenericTask


@pytest.fixture
def simple_pandas_dataframe():
    return pd.DataFrame([(42, "horse"), (101, "zebra")], columns=["id", "data"])


@pytest.fixture
def simple_pandas_series():
    return pd.Series(["horse", "zebra"])


@pytest.fixture
def quickbase_api_write_receipt():
    # example data from https://developer.quickbase.com/operation/upsert
    return {
        "data": [
            {
                "3": {"value": 1},
                "6": {"value": "Updating this record"},
                "7": {"value": 10},
                "8": {"value": "2019-12-18T08:00:00.000Z"},
            },
            {
                "3": {"value": 11},
                "6": {"value": "This is my text"},
                "7": {"value": 15},
                "8": {"value": "2019-12-19T08:00:00.000Z"},
            },
            {
                "3": {"value": 12},
                "6": {"value": "This is my other text"},
                "7": {"value": 20},
                "8": {"value": "2019-12-20T08:00:00.000Z"},
            },
        ],
        "metadata": {
            "createdRecordIds": [11, 12],
            "totalNumberOfRecordsProcessed": 3,
            "unchangedRecordIds": [],
            "updatedRecordIds": [1],
        },
    }


@pytest.fixture
def pipeline_name():
    return "Animals"


@pytest.fixture
def task_extract_animal_names(pipeline_name):
    return ExtractAnimalNames(pipeline=pipeline_name)


@pytest.fixture
def task_extract_animal_colors(pipeline_name):
    return ExtractAnimalColors(pipeline=pipeline_name)


@pytest.fixture
def task_sql_extract_animal_names(pipeline_name):
    return SQLExtractAnimalNames(pipeline=pipeline_name)


@pytest.fixture
def task_sql_extract_animal_colors(pipeline_name):
    return SQLExtractAnimalColors(pipeline=pipeline_name)


@pytest.fixture
def task_transform_animals(pipeline_name):
    return PrepareAnimals(pipeline=pipeline_name)


@pytest.fixture
def task_load_animals(pipeline_name):
    return LoadAnimals(pipeline=pipeline_name)


@pytest.fixture
def task_pipeline_animals():
    return Animals()


@pytest.fixture
def task_pipeline_animals_debug():
    return AnimalsDebug()


@pytest.fixture
def task_extract_sql_query_with_parameters(pipeline_name):
    return SQLQueryWithParameters(pipeline=pipeline_name)


@pytest.fixture
def task_pipeline_creatures():
    return Creatures()


@pytest.fixture
def task_extract_animal_names_target(targets_directory, task_extract_animal_names):
    shutil.copy(
        "tests/fixtures/targets/Animals__Extract__ExtractAnimalNames.pickle",
        task_extract_animal_names.path,
    )
    return task_extract_animal_names.target


@pytest.fixture
def task_extract_animal_colors_target(targets_directory, task_extract_animal_colors):
    shutil.copy(
        "tests/fixtures/targets/Animals__Extract__ExtractAnimalColors.pickle",
        task_extract_animal_colors.path,
    )
    return task_extract_animal_colors.target


@pytest.fixture
def task_transform_animals_target(targets_directory, task_transform_animals):
    shutil.copy(
        "tests/fixtures/targets/Animals__Transform__PrepareAnimals.pickle",
        task_transform_animals.path,
    )
    return task_transform_animals.target


@pytest.fixture
def task_load_animals_target(targets_directory, task_load_animals):
    shutil.copy(
        "tests/fixtures/targets/Animals__Load__LoadAnimals.json",
        task_load_animals.path,
    )
    return task_load_animals.target


@pytest.fixture
def qbclient():
    return QBClient()


@pytest.fixture(scope="session", autouse=True)
def global_requests_mock(request):
    if any(item.get_closest_marker("integration") for item in request.node.items):
        yield
    else:
        with requests_mock.Mocker() as m:
            yield m


@pytest.fixture
def mocked_qb_api_getApp(qbclient, global_requests_mock):
    url = f"{qbclient.api_base}/apps/{qbclient.app_id}"
    with open("tests/fixtures/qb_api_responses/getApp.json") as f:
        api_response = json.load(f)
    global_requests_mock.get(url, json=api_response)
    return api_response


@pytest.fixture
def mocked_qb_api_getAppTables(qbclient, global_requests_mock):
    url = f"{qbclient.api_base}/tables?appId={qbclient.app_id}"
    with open("tests/fixtures/qb_api_responses/getAppTables.json") as f:
        api_response = json.load(f)
    global_requests_mock.get(url, json=api_response)
    return api_response


@pytest.fixture
def mocked_table_id():
    return "bpqe82s1"


@pytest.fixture
def mocked_table_name():
    return "Example Table #0"


@pytest.fixture
def mocked_qb_api_getFields(qbclient, mocked_table_id, global_requests_mock):
    url = f"{qbclient.api_base}/fields?tableId={mocked_table_id}"
    with open("tests/fixtures/qb_api_responses/getFields.json") as f:
        api_response = json.load(f)
    global_requests_mock.get(url, json=api_response)
    return api_response


@pytest.fixture
def mocked_upsert_data():
    return [
        {"Field1": "Green", "Numeric Field": 42},
        {"Field1": "Red", "Numeric Field": 101},
        {"Field1": "Blue", "Numeric Field": 999},
    ]


@pytest.fixture
def mocked_upsert_payload(
    qbclient, mocked_table_id, mocked_upsert_data, mocked_qb_api_getFields
):
    return qbclient.prepare_upsert_payload(mocked_table_id, mocked_upsert_data, None)


@pytest.fixture
def mocked_qb_api_upsert(
    qbclient, mocked_table_id, mocked_upsert_payload, global_requests_mock
):
    url = f"{qbclient.api_base}/records"
    with open("tests/fixtures/qb_api_responses/upsert.json") as f:
        api_response = json.load(f)
    global_requests_mock.register_uri(
        "POST",
        url,
        additional_matcher=lambda req: req.json() == mocked_upsert_payload,
        json=api_response,
    )
    return api_response


@pytest.fixture
def mocked_query_all_fields_payload():
    return {
        "from": "bck7gp3q2",
        "select": [6, 7, 8],
    }


@pytest.fixture
def mocked_query_some_fields_payload():
    return {
        "from": "bck7gp3q2",
        "select": [6, 7],
    }


@pytest.fixture
def mocked_qb_api_runQuery_select_all_fields(
    qbclient, mocked_table_id, mocked_query_all_fields_payload, global_requests_mock
):
    url = f"{qbclient.api_base}/records/query"
    with open("tests/fixtures/qb_api_responses/runQuery_all_fields.json") as f:
        api_response = json.load(f)
    global_requests_mock.register_uri(
        "POST",
        url,
        additional_matcher=lambda req: req.json() == mocked_query_all_fields_payload,
        json=api_response,
    )
    return api_response


@pytest.fixture
def mocked_qb_api_runQuery_select_some_fields(
    qbclient, mocked_table_id, mocked_query_some_fields_payload, global_requests_mock
):
    url = f"{qbclient.api_base}/records/query"
    with open("tests/fixtures/qb_api_responses/runQuery_some_fields.json") as f:
        api_response = json.load(f)
    global_requests_mock.register_uri(
        "POST",
        url,
        additional_matcher=lambda req: req.json() == mocked_query_some_fields_payload,
        json=api_response,
    )
    return api_response


@pytest.fixture
def mocked_query_table_fields():
    return pd.DataFrame(
        [
            {"label": "Full Name", "id": 6},
            {"label": "Amount", "id": 7},
            {"label": "Date time", "id": 8},
        ]
    )


@pytest.fixture
def mocked_qb_api_getTable(qbclient, global_requests_mock, mocked_table_id):
    url = f"{qbclient.api_base}/tables/{mocked_table_id}?appId={qbclient.app_id}"
    with open("tests/fixtures/qb_api_responses/getTable.json") as f:
        api_response = json.load(f)
    global_requests_mock.get(url, json=api_response)
    return api_response


@pytest.fixture
def mocked_delete_payload(mocked_table_id):
    return {"from": mocked_table_id, "where": "{3.GT.0}"}


@pytest.fixture
def mocked_qb_api_delete_records(qbclient, mocked_delete_payload, global_requests_mock):
    url = f"{qbclient.api_base}/records"
    with open("tests/fixtures/qb_api_responses/deleteRecords.json") as f:
        api_response = json.load(f)
    global_requests_mock.register_uri(
        "DELETE",
        url,
        additional_matcher=lambda req: req.json() == mocked_delete_payload,
        json=api_response,
    )
    return api_response


@pytest.fixture
def qbclient_with_mocked_table_fields(qbclient, mocked_query_table_fields):
    with mock.patch.object(type(qbclient), "get_table_fields") as mocked_table_fields:
        mocked_table_fields.return_value = mocked_query_table_fields
        yield qbclient


@pytest.fixture
def mocked_transform_pandas_target(tmpdir, mocked_table_name, mocked_upsert_data):
    target = PandasPickleTarget(
        path=f"{tmpdir}/transform__example_table_0.pickle", table_name=mocked_table_name
    )
    target.write(pd.DataFrame(mocked_upsert_data))
    return target


@pytest.fixture
def quickbase_load_task_with_parent_data(mocked_transform_pandas_target):
    class LoadTaskWithData(QuickbaseUpsertTask):
        @property
        def single_input(self) -> PandasPickleTarget | QuickbaseTableTarget:
            return mocked_transform_pandas_target

    return LoadTaskWithData


@pytest.fixture
def data_warehouse_connection_string():
    return "oracle+oracledb://user1:pass1@example.org:1521/ABCDE"


@pytest.fixture
def sqlite_dwclient():
    return DWClient(connection_string="sqlite:///:memory:", engine_parameters={})


@pytest.fixture
def _qbclient_connection_test_success():
    with mock.patch.object(QBClient, "test_connection") as mocked_connection_test:
        mocked_connection_test.return_value = True
        yield


@pytest.fixture
def _dwclient_connection_test_success():
    with mock.patch.object(DWClient, "test_connection") as mocked_connection_test:
        mocked_connection_test.return_value = True
        yield


@pytest.fixture
def _dwclient_connection_test_raise_exception():
    with mock.patch.object(DWClient, "test_connection") as mocked_connection_test:
        mocked_connection_test.side_effect = Exception("Intentional Error Here")
        yield


@pytest.fixture
def _qbclient_connection_test_raise_exception():
    with mock.patch.object(QBClient, "test_connection") as mocked_connection_test:
        mocked_connection_test.side_effect = Exception("Intentional Error Here")
        yield


@pytest.fixture
def all_tasks_pipeline_name():
    return "Testing"


@pytest.fixture
def task_extract_dw_employees(all_tasks_pipeline_name):
    from hrqb.tasks.employees import ExtractDWEmployees

    return ExtractDWEmployees(pipeline=all_tasks_pipeline_name)


@pytest.fixture
def task_extract_dw_employees_dw_dataframe():
    return pd.DataFrame(
        [
            {
                "mit_id": "123456789",
                "first_name": "John",
                "last_name": "Doe",
                "preferred_name": "Johnny",
                "date_of_birth": datetime.datetime(1985, 4, 12),
                "mit_hire_date": datetime.datetime(2010, 8, 15),
                "mit_lib_hire_date": datetime.datetime(2012, 6, 20),
                "union_seniority_date": datetime.datetime(2012, 6, 20),
                "last_lib_appt_end_date": "2025-12-31",
                "home_addr_street1": "123 Elm Street",
                "home_addr_street2": "Apt 456",
                "home_addr_city": "Cambridge",
                "home_addr_state": "MA",
                "home_addr_zip": "02139",
                "home_addr_country": "USA",
                "mit_email_address": "john.doe@mit.edu",
                "office_address": "77 Massachusetts Ave, Room 4-123",
                "office_phone": "617-253-1234",
                "home_phone": "617-555-6789",
                "ethnic_origin": "Two or more races, not Hispanic/Latino",
                "gender": "female",
                "emergency_contact_name": "Jane Doe",
                "emergency_contact_relation": "Spouse",
                "emergency_contact_email": "jane.doe@example.com",
                "emergency_home_phone": "617-555-1234",
                "emergency_work_phone": "617-555-5678",
                "emergency_cell_phone": "617-555-8765",
                "highest_degree_type": "PhD",
                "highest_degree_year": "2010",
                "residency_status": "Citizen",
                "yrs_of_mit_serv": "14",
                "yrs_of_prof_expr": "20",
                "i9_form_expiration_date": np.nan,  # Null value from pandas.read_sql()
                "termination_reason": "Another Job",
            }
        ]
    )


@pytest.fixture
def task_extract_dw_employees_target(
    task_extract_dw_employees, task_extract_dw_employees_dw_dataframe
):
    task_extract_dw_employees.target.write(task_extract_dw_employees_dw_dataframe)
    return task_extract_dw_employees.target


@pytest.fixture
def task_transform_employees(all_tasks_pipeline_name):
    from hrqb.tasks.employees import TransformEmployees

    return TransformEmployees(pipeline=all_tasks_pipeline_name)


@pytest.fixture
def mocked_qbclient_departments_df():
    with mock.patch(
        "hrqb.utils.quickbase.QBClient.get_table_as_df"
    ) as mocked_get_table_df, mock.patch(
        "hrqb.utils.quickbase.QBClient.get_table_id"
    ) as _mocked_table_id:
        mocked_get_table_df.return_value = pd.DataFrame(
            [
                {
                    "Department": "Distinctive Collections",
                    "Acronym": "DDC",
                    "Record ID#": 35,
                },
                {
                    "Department": "Information Technology Services",
                    "Acronym": "ITS",
                    "Record ID#": 40,
                },
            ]
        )
        yield mocked_get_table_df


@pytest.fixture
def task_extract_qb_departments_target(
    task_extract_qb_departments, mocked_qbclient_departments_df
):
    task_extract_qb_departments.run()
    return task_extract_qb_departments.target


@pytest.fixture
def task_load_employees(all_tasks_pipeline_name):
    from hrqb.tasks.employees import LoadEmployees

    return LoadEmployees(pipeline=all_tasks_pipeline_name)


@pytest.fixture
def task_extract_dw_employee_appointment_complete(all_tasks_pipeline_name):
    from hrqb.tasks.employee_appointments import ExtractDWEmployeeAppointments

    task = ExtractDWEmployeeAppointments(pipeline=all_tasks_pipeline_name)
    task.target.write(
        pd.DataFrame(
            [
                {
                    "hr_appt_key": 123456,
                    "mit_id": "123456789",
                    "hr_org_unit_title": "Libraries",
                    "appt_begin_date": Timestamp("2021-01-01 00:00:00"),
                    "appt_end_date": datetime.datetime(2023, 12, 31, 0, 0),
                    "is_most_recent_appt": "Y",
                    "hr_job_key": "C123456789",
                    "job_id": "123456789",
                    "job_title_long": "Software Developer 1",
                    "employee_group": "Exempt",
                    "exempt": "E",
                    "job_family": "Information Technology",
                    "job_subfamily": "Application Development",
                    "job_track": "Individual",
                    "pay_grade": "5",
                    "hr_position_key": "S987654321",
                    "position_id": "987654321",
                    "position_title_long": "Developer",
                    "employee_type": "Admin Staff",
                    "union_name": "Admin Staff",
                    "term_or_perm": "Permanent",
                    "benefits_group_type": "Standard Benefits",
                }
            ]
        )
    )
    return task


@pytest.fixture
def task_transform_employee_appointments_complete(
    all_tasks_pipeline_name,
    task_extract_dw_employee_appointment_complete,
):
    from hrqb.tasks.employee_appointments import TransformEmployeeAppointments

    task = TransformEmployeeAppointments(pipeline=all_tasks_pipeline_name)
    task.run()
    return task


@pytest.fixture
def task_load_employee_appointments(
    all_tasks_pipeline_name, task_transform_employee_appointments_complete
):
    from hrqb.tasks.employee_appointments import LoadEmployeeAppointments

    return LoadEmployeeAppointments(pipeline=all_tasks_pipeline_name)


@pytest.fixture
def task_transform_employee_types_complete(
    all_tasks_pipeline_name, task_extract_dw_employee_appointment_complete
):
    from hrqb.tasks.employee_types import TransformEmployeeTypes

    task = TransformEmployeeTypes(pipeline=all_tasks_pipeline_name)
    task.run()
    return task


@pytest.fixture
def task_transform_job_titles_complete(
    all_tasks_pipeline_name, task_extract_dw_employee_appointment_complete
):
    from hrqb.tasks.job_titles import TransformUniqueJobTitles

    task = TransformUniqueJobTitles(pipeline=all_tasks_pipeline_name)
    task.run()
    return task


@pytest.fixture
def task_transform_position_titles_complete(
    all_tasks_pipeline_name, task_extract_dw_employee_appointment_complete
):
    from hrqb.tasks.position_titles import TransformUniquePositionTitles

    task = TransformUniquePositionTitles(pipeline=all_tasks_pipeline_name)
    task.run()
    return task


@pytest.fixture
def task_extract_dw_employee_salary_history_complete(all_tasks_pipeline_name):
    from hrqb.tasks.employee_salary_history import ExtractDWEmployeeSalaryHistory

    task = ExtractDWEmployeeSalaryHistory(pipeline=all_tasks_pipeline_name)
    task.target.write(
        pd.DataFrame(
            [
                {
                    "hr_appt_key": 123,
                    "hr_appt_tx_key": "00000000ABCDEFGHIJ1234567890",
                    "mit_id": "123456789",
                    "job_id": "123456789",
                    "position_id": "987654321",
                    "appt_begin_date": Timestamp("2019-01-01 00:00:00"),
                    "appt_end_date": datetime.datetime(2022, 12, 1, 0, 0),
                    "start_date": Timestamp("2019-07-01 00:00:00"),
                    "end_date": datetime.datetime(2022, 12, 1, 0, 0),
                    "hr_personnel_action_type_key": "CS01",
                    "hr_personnel_action": "Annual Salary Review",
                    "hr_action_reason": "Review Increase",
                    "original_base_amount": 50000.0,
                    "original_hourly_rate": 27.77,
                    "original_effort": 100.0,
                    "temp_change_base_amount": 0.0,
                    "temp_change_hourly_rate": 0.0,
                    "temp_effort": 0.0,
                    "temp_base_change_percent": 0.0,
                    "special_onetime_pay": "N",
                }
            ]
        )
    )
    return task


@pytest.fixture
def task_shared_extract_qb_employee_appointments_complete(all_tasks_pipeline_name):
    from hrqb.tasks.shared import ExtractQBEmployeeAppointments

    task = ExtractQBEmployeeAppointments(pipeline=all_tasks_pipeline_name)
    task.target.write(
        pd.DataFrame(
            [
                {
                    "Record ID#": 12000,
                    "Position ID": "987654321",
                    "Begin Date": "2019-01-01",
                    "End Date": "2022-12-01",
                    "MIT ID": "123456789",
                    "Related Employee Type": "Admin Staff",
                    "Union Name": "Le Union",
                    "Exempt / NE": "E",
                    "Key": "00f6099d6777bd9b6985bf86eeb3e449",
                }
            ]
        )
    )
    return task


@pytest.fixture
def task_transform_employee_salary_history_complete(
    all_tasks_pipeline_name,
    task_extract_dw_employee_salary_history_complete,
    task_shared_extract_qb_employee_appointments_complete,
):
    from hrqb.tasks.employee_salary_history import TransformEmployeeSalaryHistory

    task = TransformEmployeeSalaryHistory(pipeline=all_tasks_pipeline_name)
    task.run()
    return task


@pytest.fixture
def task_load_employee_salary_history_complete(
    all_tasks_pipeline_name, task_transform_employee_salary_history_complete
):
    from hrqb.tasks.employee_salary_history import LoadEmployeeSalaryHistory

    return LoadEmployeeSalaryHistory(pipeline=all_tasks_pipeline_name)


@pytest.fixture
def task_transform_employee_salary_change_types_complete(
    all_tasks_pipeline_name, task_extract_dw_employee_salary_history_complete
):
    from hrqb.tasks.salary_change_types import TransformSalaryChangeTypes

    task = TransformSalaryChangeTypes(pipeline=all_tasks_pipeline_name)
    task.run()
    return task


@pytest.fixture
def task_extract_dw_employee_leave_complete(all_tasks_pipeline_name):
    from hrqb.tasks.employee_leave import ExtractDWEmployeeLeave

    task = ExtractDWEmployeeLeave(pipeline=all_tasks_pipeline_name)
    task.target.write(
        pd.DataFrame(
            [
                {
                    "mit_id": "123456789",
                    "appt_begin_date": Timestamp("2019-01-01 00:00:00"),
                    "appt_end_date": datetime.datetime(2022, 12, 1, 0, 0),
                    "position_id": "987654321",
                    "hr_appt_key": 123,
                    "absence_date": Timestamp("2019-07-01 00:00:00"),
                    "absence_type": "Vacation",
                    "absence_type_code": "VACA",
                    "actual_absence_hours": 8.0,
                    "actual_absence_days": 1.0,
                    "paid_leave": "Y",
                }
            ]
        )
    )
    return task


@pytest.fixture
def task_transform_employee_leave_complete(
    all_tasks_pipeline_name,
    task_extract_dw_employee_leave_complete,
    task_shared_extract_qb_employee_appointments_complete,
):
    from hrqb.tasks.employee_leave import TransformEmployeeLeave

    task = TransformEmployeeLeave(pipeline=all_tasks_pipeline_name)
    task.run()
    return task


@pytest.fixture
def task_load_employee_leave(
    all_tasks_pipeline_name, task_transform_employee_leave_complete
):
    from hrqb.tasks.employee_leave import LoadEmployeeLeave

    return LoadEmployeeLeave(pipeline=all_tasks_pipeline_name)


@pytest.fixture
def task_transform_employee_leave_types_complete(
    all_tasks_pipeline_name,
    task_transform_employee_leave_complete,
):
    from hrqb.tasks.employee_leave_types import TransformEmployeeLeaveTypes

    task = TransformEmployeeLeaveTypes(pipeline=all_tasks_pipeline_name)
    task.run()
    return task


@pytest.fixture
def task_transform_performance_reviews_complete(
    all_tasks_pipeline_name,
    task_shared_extract_qb_employee_appointments_complete,
):
    from hrqb.tasks.performance_reviews import TransformPerformanceReviews

    task = TransformPerformanceReviews(pipeline=all_tasks_pipeline_name)
    task.run()
    return task


@pytest.fixture
def task_load_performance_reviews_complete(
    all_tasks_pipeline_name,
    task_transform_performance_reviews_complete,
):
    from hrqb.tasks.performance_reviews import LoadPerformanceReviews

    return LoadPerformanceReviews(pipeline=all_tasks_pipeline_name)


@pytest.fixture
def task_transform_years_complete(
    all_tasks_pipeline_name,
    task_transform_performance_reviews_complete,
):
    from hrqb.tasks.years import TransformYears

    task = TransformYears(pipeline=all_tasks_pipeline_name)
    task.run()
    return task


@pytest.fixture
def sensitive_scope_variable():
    return {
        "note": "I am a dictionary with sensitive information",
        "secret": "very-secret-abc123",
    }


@pytest.fixture
def pandas_task_with_integrity_checks():
    class PandasWithChecks(PandasPickleTask):
        stage = luigi.Parameter("Transform")

        def get_dataframe(self) -> pd.DataFrame:
            return pd.DataFrame(
                [0, 1, 2, 3, 4],
                columns=["number"],
            )

        @HRQBTask.integrity_check
        def expecting_five_items(self, output_df: pd.DataFrame):
            if len(output_df) != 5:
                raise ValueError("Expecting a dataframe with five rows")

        @HRQBTask.integrity_check
        def expecting_letter_column(self, output_df: pd.DataFrame):
            if "letter" not in output_df.columns:
                raise ValueError("Expecting a 'letter' column in dataframe")

    return PandasWithChecks(pipeline="Checks")


@pytest.fixture
def upsert_task_with_integrity_checks(mocked_qb_api_upsert):
    class UpsertWithChecks(QuickbaseUpsertTask):
        stage = luigi.Parameter("Load")

        def get_records(self) -> pd.DataFrame:
            return None

        def upsert_records(self, _records):
            return mocked_qb_api_upsert

        @HRQBTask.integrity_check
        def expecting_three_processed_records(self, upsert_results: dict):
            if upsert_results["metadata"]["totalNumberOfRecordsProcessed"] != 3:
                raise ValueError("Expecting three processed records")

        @HRQBTask.integrity_check
        def expecting_zero_updated_records(self, upsert_results: dict):
            if len(upsert_results["metadata"]["updatedRecordIds"]) != 0:
                raise ValueError("Expecting zero updated records")

    return UpsertWithChecks(pipeline="Checks")


@pytest.fixture
def upsert_task_with_duplicate_merge_field_values(mocked_qb_api_upsert):
    class UpsertWithDuplicates(QuickbaseUpsertTask):
        stage = luigi.Parameter("Load")

        @property
        def merge_field(self) -> str | None:
            return "Key"

        @property
        def single_input_dataframe(self) -> pd.DataFrame:
            return pd.DataFrame(
                ["abc123", "def456", "abc123"],
                columns=["Key"],
            )

    return UpsertWithDuplicates(pipeline="Checks")


@pytest.fixture
def task_extract_dw_employee_leave_balances_complete(all_tasks_pipeline_name):
    from hrqb.tasks.employee_leave_balances import ExtractDWEmployeeLeaveBalances

    task = ExtractDWEmployeeLeaveBalances(pipeline=all_tasks_pipeline_name)
    task.target.write(
        pd.DataFrame(
            [
                {
                    "mit_id": "123456789",
                    "balance_type": "MIT Non-Ex Vacation Quota",
                    "beginning_balance_hours": 80.0,
                    "deducted_hours": 8.0,
                    "ending_balance_hours": 72.0,
                    "beginning_balance_days": 10.0,
                    "deducted_days": 1.0,
                    "ending_balance_days": 9.0,
                    "absence_balance_begin_date": "2006-06-26",
                    "absence_balance_end_date": "2999-12-31",
                }
            ]
        )
    )
    return task


@pytest.fixture
def task_transform_employee_leave_balance_complete(
    all_tasks_pipeline_name,
    task_extract_dw_employee_leave_balances_complete,
):
    from hrqb.tasks.employee_leave_balances import TransformEmployeeLeaveBalances

    task = TransformEmployeeLeaveBalances(pipeline=all_tasks_pipeline_name)
    task.run()
    return task


@pytest.fixture
def task_load_employee_leave_balances(
    all_tasks_pipeline_name, task_transform_employee_leave_balance_complete
):
    from hrqb.tasks.employee_leave_balances import LoadEmployeeLeaveBalances

    return LoadEmployeeLeaveBalances(pipeline=all_tasks_pipeline_name)
