"""tests.oracle.test_connections

These integration tests will bypass monkey-patched env vars and require real values
for the following env vars:
    - QUICKBASE_API_URL
    - QUICKBASE_API_TOKEN
    - QUICKBASE_APP_ID
    - DATA_WAREHOUSE_CONNECTION_STRING
"""

import oracledb
import pytest

from hrqb.utils.data_warehouse import DWClient
from hrqb.utils.quickbase import QBClient


@pytest.mark.integration()
def test_integration_oracle_client_installed_success():
    oracledb.init_oracle_client()


@pytest.mark.integration()
def test_integration_data_warehouse_connection_success():
    dwclient = DWClient()
    assert dwclient.test_connection()


@pytest.mark.integration()
def test_integration_quickbase_api_connection_success():
    qbclient = QBClient()
    assert qbclient.test_connection()
