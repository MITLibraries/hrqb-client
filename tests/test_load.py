# ruff: noqa: N803

import pytest

from hrqb.base import QuickbaseUpsertTask


def test_load_upsert_records_default_get_records_no_parent_error(
    tmpdir,
    mocked_table_name,
):
    task = QuickbaseUpsertTask(
        path=f"{tmpdir}/load__example_table_0.json",
        table_name=mocked_table_name,
    )
    with pytest.raises(
        ValueError, match="Expected a single input to this Task but found: 0"
    ):
        task.get_records()


def test_load_upsert_records_default_get_records_returns_dict(
    tmpdir,
    quickbase_load_task_with_parent_data,
    mocked_table_name,
    mocked_upsert_data,
):
    task = quickbase_load_task_with_parent_data(
        path=f"{tmpdir}/load__example_table_0.json",
        table_name=mocked_table_name,
    )
    assert task.get_records() == mocked_upsert_data


def test_load_upsert_records_task_run_success(
    tmpdir,
    quickbase_load_task_with_parent_data,
    mocked_table_name,
    mocked_qb_api_getAppTables,
    mocked_qb_api_upsert,
):

    task = quickbase_load_task_with_parent_data(
        path=f"{tmpdir}/load__example_table_0.json",
        table_name=mocked_table_name,
    )
    task.run()
    assert task.complete()
    assert task.output().read() == mocked_qb_api_upsert
