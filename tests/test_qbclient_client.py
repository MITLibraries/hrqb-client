# ruff: noqa: N803, PLR2004, PD901, SLF001

import os
from unittest import mock

import pandas as pd
import pytest
import requests
from requests.exceptions import HTTPError
from requests.models import Response

from hrqb.exceptions import QBFieldNotFoundError


def test_qbclient_init_defaults_from_env_vars(qbclient):
    assert (
        qbclient.request_headers["Authorization"]
        == f"QB-USER-TOKEN {os.environ['QUICKBASE_API_TOKEN']}"
    )
    assert qbclient.app_id == os.environ["QUICKBASE_APP_ID"]
    assert qbclient.api_base == os.environ["QUICKBASE_API_URL"]


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


def test_qbclient_get_table(qbclient, mocked_table_id, mocked_qb_api_getTable):
    assert qbclient.get_table(mocked_table_id) == mocked_qb_api_getTable


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


def test_qbclient_query_records_all_fields(
    qbclient, mocked_query_all_fields_payload, mocked_qb_api_runQuery_select_all_fields
):
    assert (
        qbclient.query_records(mocked_query_all_fields_payload)
        == mocked_qb_api_runQuery_select_all_fields
    )


def test_qbclient_query_records_some_fields(
    qbclient, mocked_query_some_fields_payload, mocked_qb_api_runQuery_select_some_fields
):
    assert (
        qbclient.query_records(mocked_query_some_fields_payload)
        == mocked_qb_api_runQuery_select_some_fields
    )


def test_qbclient_query_records_mapped_fields_iter_all_fields(
    qbclient, mocked_query_all_fields_payload, mocked_qb_api_runQuery_select_all_fields
):
    records = list(
        qbclient.query_records_mapped_fields_iter(mocked_query_all_fields_payload)
    )
    assert records == [
        {"Full Name": "Andre Harris", "Date time": "2019-12-18T08:00:00Z", "Amount": 10}
    ]


def test_qbclient_query_records_mapped_fields_iter_some_fields(
    qbclient, mocked_query_some_fields_payload, mocked_qb_api_runQuery_select_some_fields
):
    records = list(
        qbclient.query_records_mapped_fields_iter(mocked_query_some_fields_payload)
    )
    assert records == [{"Full Name": "Andre Harris", "Amount": 10}]


def test_qbclient_get_table_as_df_all_fields(qbclient_with_mocked_table_fields):
    records_df = qbclient_with_mocked_table_fields.get_table_as_df("bck7gp3q2")
    assert isinstance(records_df, pd.DataFrame)
    assert list(records_df.columns) == ["Full Name", "Amount", "Date time"]
    assert records_df.iloc[0]["Full Name"] == "Andre Harris"


def test_qbclient_get_table_as_df_some_fields(qbclient_with_mocked_table_fields):
    fields = ["Full Name", "Amount"]
    records_df = qbclient_with_mocked_table_fields.get_table_as_df(
        "bck7gp3q2",
        fields=fields,
    )
    assert list(records_df.columns) == fields
    assert "Date time" not in records_df.columns


def test_qbclient_api_non_2xx_response_error(qbclient):
    with mock.patch.object(requests, "get") as mocked_get:
        response = Response()
        response.status_code = 400
        mocked_get.return_value = response
        with pytest.raises(
            requests.RequestException, match="Quickbase API error - status 400"
        ):
            assert qbclient.make_request(requests.get, "/always/fail")


def test_qbclient_test_connection_success(qbclient):
    assert qbclient.test_connection()


def test_qbclient_test_connection_connection_error(qbclient):
    error_message = "Invalid API token"
    with mock.patch.object(type(qbclient), "make_request") as mocked_make_request:
        mocked_make_request.side_effect = HTTPError(error_message)
        with pytest.raises(HTTPError, match=error_message):
            qbclient.test_connection()


def test_qbclient_test_connection_response_error(qbclient):
    with mock.patch.object(type(qbclient), "make_request") as mocked_make_request:
        mocked_make_request.return_value = {"msg": "this is not expected"}
        with pytest.raises(ValueError, match="API returned unexpected response"):
            qbclient.test_connection()
