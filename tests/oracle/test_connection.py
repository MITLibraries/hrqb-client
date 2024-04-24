"""tests.oracle.test_connection"""

import oracledb


def test_oracle_client_installed_success():
    oracledb.init_oracle_client()
