# ruff: noqa: PD901, PLR2004

import pandas as pd
import pytest
from sqlalchemy.engine import Engine

from hrqb.utils.data_warehouse import DWClient

# NOTE: these tests either exercise the DWClient itself, or use an embedded SQLite
#   database, and therefore do not require the full Oracle "thick" client to be installed
#   and successfully loaded (which are handled as separate, optional integration tests).


def test_dwclient_default_engine_parameters():
    assert DWClient.default_engine_parameters() == {"thick_mode": True}


def test_dwclient_validate_connection_string_explicit_success(
    monkeypatch, data_warehouse_connection_string
):
    monkeypatch.delenv("DATA_WAREHOUSE_CONNECTION_STRING")
    dwclient = DWClient(connection_string=data_warehouse_connection_string)
    assert dwclient.validate_data_warehouse_connection_string() is None


def test_dwclient_validate_connection_string_env_var_success(
    monkeypatch, data_warehouse_connection_string
):
    dwclient = DWClient(connection_string=data_warehouse_connection_string)
    assert dwclient.validate_data_warehouse_connection_string() is None


def test_dwclient_validate_connection_string_missing_error(
    monkeypatch, data_warehouse_connection_string
):
    monkeypatch.delenv("DATA_WAREHOUSE_CONNECTION_STRING")
    dwclient = DWClient()
    with pytest.raises(AttributeError, match="connection string not found"):
        dwclient.validate_data_warehouse_connection_string()


def test_dwclient_sqlite_connection_string_and_engine_success(sqlite_dwclient):
    assert sqlite_dwclient.engine is None
    sqlite_dwclient.init_engine()
    assert isinstance(sqlite_dwclient.engine, Engine)


def test_dwclient_engine_created_automatically_for_queries(sqlite_dwclient):
    assert sqlite_dwclient.engine is None
    sqlite_dwclient.execute_query("""select 1 as foo""")
    assert isinstance(sqlite_dwclient.engine, Engine)


def test_dwclient_execute_query_return_dataframe_success(sqlite_dwclient):
    df = sqlite_dwclient.execute_query("""select 1 as foo""")
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0].foo == 1


def test_dwclient_execute_query_accepts_sql_parameters(sqlite_dwclient):
    foo_val, bar_val = 42, "apple"
    df = sqlite_dwclient.execute_query(
        """select :foo_val as foo, :bar_val as bar""",
        params={"foo_val": foo_val, "bar_val": bar_val},
    )
    assert df.iloc[0].foo == foo_val
    assert df.iloc[0].bar == bar_val
