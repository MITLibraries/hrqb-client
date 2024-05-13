"""hrqb.utils.data_warehouse"""

import logging

import pandas as pd
from attrs import define, field
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from hrqb.config import Config

logger = logging.getLogger(__name__)


@define
class DWClient:
    """Client to provide Oracle Data Warehouse connection and querying."""

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

    def init_engine(self) -> None:
        """Instantiate a SQLAlchemy engine if not already configured and set.

        User provided engine parameters will override self.default_engine_parameters.
        """
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
