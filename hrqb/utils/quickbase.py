"""utils.quickbase"""

import json
import logging
from collections.abc import Callable

import pandas as pd
import requests
from attrs import define, field

from hrqb.config import Config
from hrqb.exceptions import QBFieldNotFoundError

logger = logging.getLogger(__name__)

RequestsMethod = Callable[..., requests.Response]


@define
class QBClient:
    api_base: str = field(default="https://api.quickbase.com/v1")
    cache_results: bool = field(default=True)
    _cache: dict = field(factory=dict, repr=False)

    @property
    def request_headers(self) -> dict:
        return {
            "Authorization": f"QB-USER-TOKEN {Config().QUICKBASE_API_TOKEN}",
            "QB-Realm-Hostname": "mit.quickbase.com",
        }

    @property
    def app_id(self) -> str:
        return Config().QUICKBASE_APP_ID

    def make_request(
        self, requests_method: RequestsMethod, path: str, **kwargs: dict
    ) -> dict:
        """Make an API request to Quickbase API.

        This method caches request responses, such that data from informational requests
        may be reused in later operations.
        """
        # hash the request to cache the response
        request_hash = (path, json.dumps(kwargs, sort_keys=True))
        if self.cache_results and request_hash in self._cache:
            message = f"Using cached result for path: {path}"
            logger.debug(message)
            return self._cache[request_hash]

        # make API call
        results = requests_method(
            f"{self.api_base}/{path.removeprefix('/')}",
            headers=self.request_headers,
            **kwargs,
        ).json()
        if self.cache_results:
            self._cache[request_hash] = results

        return results

    def get_app_info(self) -> dict:
        """Retrieve information about the QB app.

        https://developer.quickbase.com/operation/getApp
        """
        return self.make_request(requests.get, f"apps/{self.app_id}")

    def get_tables(self) -> pd.DataFrame:
        """Get all QB Tables as a Dataframe.

        https://developer.quickbase.com/operation/getAppTables
        """
        tables = self.make_request(requests.get, f"tables?appId={self.app_id}")
        return pd.DataFrame(tables)

    def get_table_id(self, name: str) -> str:
        """Get Table ID from Dataframe of Tables."""
        tables_df = self.get_tables()
        return tables_df[tables_df.name == name].iloc[0].id

    def get_table_fields(self, table_id: str) -> pd.DataFrame:
        """Get all QB Table Fields as a Dataframe.

        https://developer.quickbase.com/operation/getFields
        """
        fields = self.make_request(requests.get, f"fields?tableId={table_id}")
        return pd.DataFrame(fields)

    def get_table_fields_label_to_id(self, table_id: str) -> dict:
        """Get Field label-to-id map for a Table.

        This method is particularly helpful for upserting data via the QB API, where
        Field IDs are required instead of Field labels.
        """
        fields_df = self.get_table_fields(table_id)
        return {f["label"]: f["id"] for _, f in fields_df.iterrows()}

    def upsert_records(self, upsert_payload: dict) -> dict:
        """Upsert Records into a Table.

        https://developer.quickbase.com/operation/upsert
        """
        return self.make_request(requests.post, "records", json=upsert_payload)

    def prepare_upsert_payload(
        self,
        table_id: str,
        records: list[dict],
        merge_field: str | None = None,
    ) -> dict:
        """Prepare an API payload for upsert.

        https://developer.quickbase.com/operation/upsert

        This method expects a list of dictionaries, one dictionary per record, with a
        {Field Label:Value} structure.  This method will first retrieve a mapping of
        Field label-to-ID mapping, then remap the data to a {Field ID:Value} structure.

        Then, return a dictionary payload suitable for the QB upsert API call.
        """
        field_map = self.get_table_fields_label_to_id(table_id)
        mapped_records = []
        for record in records:
            mapped_record = {}
            for field_label, field_value in record.items():
                if field_id := field_map.get(field_label):
                    mapped_record[str(field_id)] = {"value": field_value}
                else:
                    message = (
                        f"Field label '{field_label}' not found for Table ID '{table_id}'"
                    )
                    raise QBFieldNotFoundError(message)
            mapped_records.append(mapped_record)

        upsert_payload = {
            "to": table_id,
            "data": mapped_records,
            "fieldsToReturn": list(field_map.values()),
        }
        if merge_field:
            upsert_payload["mergeFieldId"] = field_map[merge_field]

        return upsert_payload
