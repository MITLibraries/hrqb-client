# ruff: noqa: N803, PLR2004, PD901, SLF001

import os
from unittest import mock

import pandas as pd
import pytest

from hrqb.exceptions import QBFieldNotFoundError


def test_qbclient_init(qbclient):
    assert (
        qbclient.request_headers["Authorization"]
        == f"QB-USER-TOKEN {os.environ['QUICKBASE_API_TOKEN']}"
    )
    assert qbclient.app_id == os.environ["QUICKBASE_APP_ID"]


def test_qbclient_cache_api_request_response(qbclient, mocked_qb_api_getApp):
    assert qbclient._cache == {}
    first_response = qbclient.get_app_info()
    with mock.patch("requests.get") as mocked_requests_get:
        mocked_requests_get.return_value = mocked_qb_api_getApp
        second_response = qbclient.get_app_info()
    assert first_response == second_response
    assert ("apps/qb-app-def456", "{}") in qbclient._cache
    mocked_requests_get.assert_not_called()


def test_qbclient_get_app_info(qbclient, mocked_qb_api_getApp):
    assert qbclient.get_app_info() == mocked_qb_api_getApp


def test_qbclient_get_tables(qbclient, mocked_qb_api_getAppTables):
    assert len(mocked_qb_api_getAppTables) == 2
    assert mocked_qb_api_getAppTables[0]["name"] == "Example Table #0"
    df = pd.DataFrame(mocked_qb_api_getAppTables)
    assert df.equals(qbclient.get_tables())


def test_qbclient_get_table_id(qbclient, mocked_qb_api_getAppTables, mocked_table_id):
    assert qbclient.get_table_id("Example Table #0") == mocked_table_id


def test_qbclient_get_table_fields(qbclient, mocked_qb_api_getFields, mocked_table_id):
    assert len(mocked_qb_api_getFields) == 2
    assert mocked_qb_api_getFields[0]["label"] == "Field1"
    df = pd.DataFrame(mocked_qb_api_getFields)
    assert df.equals(qbclient.get_table_fields(mocked_table_id))


def test_qbclient_get_table_fields_name_to_id(
    qbclient,
    mocked_table_id,
    mocked_qb_api_getFields,
):
    field_name_to_ids = qbclient.get_table_fields_label_to_id(mocked_table_id)
    assert field_name_to_ids["Field1"] == 1
    assert field_name_to_ids["Numeric Field"] == 2


def test_qbclient_convert_record_field_labels_to_ids(qbclient):
    records = [{"record_id": 42, "note": "Hello World"}]
    field_map = {"record_id": "RecordId", "note": "Message"}
    assert qbclient.map_and_format_records_for_upsert(field_map, records) == [
        {"RecordId": {"value": 42}, "Message": {"value": "Hello World"}}
    ]


def test_qbclient_prepare_upsert_payload(
    qbclient, mocked_table_id, mocked_upsert_data, mocked_upsert_payload
):
    upsert_payload = qbclient.prepare_upsert_payload(
        mocked_table_id, mocked_upsert_data, None
    )
    assert upsert_payload == mocked_upsert_payload


def test_qbclient_prepare_upsert_payload_sets_merge_field(
    qbclient, mocked_table_id, mocked_upsert_data
):
    merge_field_name, merge_field_id = "Numeric Field", 2
    upsert_payload = qbclient.prepare_upsert_payload(
        mocked_table_id, mocked_upsert_data, merge_field=merge_field_name
    )
    assert upsert_payload["mergeFieldId"] == merge_field_id


def test_qbclient_upsert_records_bad_field_name_error(
    qbclient, mocked_table_id, mocked_qb_api_getFields
):
    records = [
        {"Field1": "Green", "Numeric Field": 42},
        {"Field1": "Red", "Numeric Field": 101},
        {"Field1": "Blue", "Numeric Field": 999},
        {"Field1": "Blue", "Numeric Field": 999, "IAmBadField": "I will error."},
    ]
    with pytest.raises(QBFieldNotFoundError, match="Field label 'IAmBadField' not found"):
        qbclient.prepare_upsert_payload(mocked_table_id, records, merge_field=None)


def test_qbclient_upsert_records_success(
    qbclient,
    mocked_qb_api_upsert,
    mocked_upsert_payload,
):
    assert qbclient.upsert_records(mocked_upsert_payload) == mocked_qb_api_upsert
