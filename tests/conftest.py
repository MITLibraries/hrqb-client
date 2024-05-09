# ruff: noqa: N802, N803

import json
import shutil

import luigi
import pandas as pd
import pytest
import requests_mock
from click.testing import CliRunner

from hrqb.base import HRQBTask, QuickbaseTableTarget
from hrqb.base.task import PandasPickleTarget, QuickbaseUpsertTask
from hrqb.utils.data_warehouse import DWClient
from hrqb.utils.quickbase import QBClient
from tests.fixtures.tasks.extract import (
    ExtractAnimalColors,
    ExtractAnimalNames,
    SQLExtractAnimalColors,
    SQLExtractAnimalNames,
)
from tests.fixtures.tasks.load import LoadAnimals
from tests.fixtures.tasks.pipelines import Animals, AnimalsDebug
from tests.fixtures.tasks.transform import PrepareAnimals


@pytest.fixture(autouse=True)
def _test_env(monkeypatch, targets_directory, data_warehouse_connection_string):
    monkeypatch.setenv("SENTRY_DSN", "None")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("LUIGI_CONFIG_PATH", "hrqb/luigi.cfg")
    monkeypatch.setenv("QUICKBASE_API_TOKEN", "qb-api-acb123")
    monkeypatch.setenv("QUICKBASE_APP_ID", "qb-app-def456")
    monkeypatch.setenv("TARGETS_DIRECTORY", str(targets_directory))
    monkeypatch.setenv(
        "DATA_WAREHOUSE_CONNECTION_STRING",
        data_warehouse_connection_string,
    )


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
def task_pipeline_animals(pipeline_name):
    return Animals()


@pytest.fixture
def task_pipeline_animals_debug(pipeline_name):
    return AnimalsDebug()


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
def global_requests_mock():
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
    return "oracle+oracledb://user1:pass1@warehouse.mit.edu:1521/DWRHS"


@pytest.fixture
def sqlite_dwclient():
    return DWClient(connection_string="sqlite:///:memory:", engine_parameters={})
