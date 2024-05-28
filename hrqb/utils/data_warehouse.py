"""hrqb.utils.data_warehouse"""

# ruff: noqa: PLR2004

import logging

import pandas as pd
from attrs import define, field
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from hrqb.config import Config

logger = logging.getLogger(__name__)


@define
class DWClient:
    """Client to provide Oracle Data Warehouse connection and querying.

    Fields:
        connection_string: str
            - full SQLAlchemy connection string, e.g.
                - oracle: oracle+oracledb://user1:pass1@example.org:1521/ABCDE
                - sqlite: sqlite:///:memory:
            - defaults to env var DATA_WAREHOUSE_CONNECTION_STRING, loaded from env vars
            at time of DWClient initialization
        engine_parameters: dict
            - optional dictionary of SQLAlchemy engine parameters
        engine: Engine
            - set via self.init_engine()
    """

    connection_string: str = field(
        factory=lambda: Config().DATA_WAREHOUSE_CONNECTION_STRING,
        repr=False,
    )
    engine_parameters: dict = field(factory=lambda: {"thick_mode": True})
    engine: Engine = field(default=None)

    def verify_connection_string_set(self) -> None:
        """Verify that a connection string is set explicitly or by env var default."""
        if not self.connection_string:
            message = (
                "Data Warehouse connection string not found.  Please pass explicitly to "
                "DWClient or set env var DATA_WAREHOUSE_CONNECTION_STRING."
            )
            raise AttributeError(message)

    def test_connection(self) -> bool:
        """Test connection to Data Warehouse.

        Executes a simple SQL statement that should return a simple dataframe if a valid
        connection is established.
        """
        if self.connection_string.startswith("sqlite"):
            query = """select 1 as x, 2 as y"""
        else:
            query = """select 1 as x, 2 as y from dual"""
        self.execute_query(query)
        return True

    def init_engine(self) -> None:
        """Instantiate a SQLAlchemy engine if not already configured and set."""
        self.verify_connection_string_set()
        if not self.engine:
            self.engine = create_engine(self.connection_string, **self.engine_parameters)

    def execute_query(self, query: str, params: dict | None = None) -> pd.DataFrame:
        """Execute SQL query, with optional parameters, returning a pandas Dataframe.

        Example:
            query = "SELECT * FROM my_table WHERE name = :name AND age > :age"
            params = {'name': 'Alice', 'age': 30}
            results = client.execute_query(query, params)
        """
        self.init_engine()
        return pd.read_sql(query, con=self.engine, params=params)
