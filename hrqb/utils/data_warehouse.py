"""hrqb.utils.data_warehouse"""

import logging

import pandas as pd
from attrs import define, field
from sqlalchemy import (
    Engine,
    create_engine,
)

from hrqb.config import Config

logger = logging.getLogger(__name__)


@define
class DWClient:
    """Client to provide Oracle Data Warehouse connection and querying."""

    connection_string: str = field(
        default=Config().DATA_WAREHOUSE_CONNECTION_STRING, repr=False
    )
    engine_parameters: dict | None = field(default=None)
    engine: Engine = field(default=None)

    @staticmethod
    def default_engine_parameters() -> dict:
        return {"thick_mode": True}

    def validate_data_warehouse_connection_string(self) -> None:
        """Validates that a proper connection is configured."""
        if not self.connection_string:
            message = (
                "Data Warehouse connection string not found.  Please pass explicitly to "
                "DWClient or set env var DATA_WAREHOUSE_CONNECTION_STRING."
            )
            raise RuntimeError(message)

    def init_engine(self) -> None:
        """Instantiate a SQLAlchemy engine if not already configured and set.

        User provided engine parameters will override self.default_engine_parameters.
        """
        self.validate_data_warehouse_connection_string()
        if not self.engine:
            engine_parameters = (
                self.engine_parameters
                if self.engine_parameters is not None
                else self.default_engine_parameters()
            )
            self.engine = create_engine(self.connection_string, **engine_parameters)

    def execute_query(self, query: str, params: dict | None = None) -> pd.DataFrame:
        """Execute SQL query, with optional parameters, returning a pandas Dataframe.

        Example:
            query = "SELECT * FROM my_table WHERE name = :name AND age > :age"
            params = {'name': 'Alice', 'age': 30}
            results = client.execute_query(query, params)
        """
        self.init_engine()
        return pd.read_sql(query, con=self.engine, params=params)
