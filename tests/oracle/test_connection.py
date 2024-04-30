"""tests.oracle.test_connection"""

import oracledb
import pytest


@pytest.mark.integration()
def test_oracle_client_installed_success():
    oracledb.init_oracle_client()
