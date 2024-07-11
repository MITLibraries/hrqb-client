"""utils.quickbase"""

import json
import logging
from collections import defaultdict
from collections.abc import Callable, Iterator

import pandas as pd
import requests
from attrs import define, field
from requests.exceptions import RequestException

from hrqb.config import Config
from hrqb.exceptions import QBFieldNotFoundError

logger = logging.getLogger(__name__)

RequestsMethod = Callable[..., requests.Response]


@define
class QBClient:
    api_base: str = field(factory=lambda: Config().QUICKBASE_API_URL)
    cache_results: bool = field(default=True)
    _cache: dict = field(factory=dict, repr=False)

    def test_connection(self) -> bool:
        """Test connection to Quickbase API.

        Retrieves Quickbase app information and checks for known key to establish
        successful authentication and app permissions.
        """
        results = self.get_app_info()
        if results.get("id") != Config().QUICKBASE_APP_ID:
            message = f"API returned unexpected response: {results}"
            raise ValueError(message)
        return True

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
        self,
        requests_method: RequestsMethod,
        path: str,
        cache: bool = True,  # noqa: FBT001, FBT002
        **kwargs: dict,
    ) -> dict:
        """Make an API request to Quickbase API.

        This method caches request responses, such that data from informational requests
        may be reused in later operations.  Cached responses only persist for the life
        of the instantiated QBClient object.
        """
        # hash the request to cache the response
        request_hash = (path, json.dumps(kwargs, sort_keys=True))
        if cache and self.cache_results and request_hash in self._cache:
            message = f"Using cached result for path: {path}"
            logger.debug(message)
            return self._cache[request_hash]

        # make API call
        response = requests_method(
            f"{self.api_base}/{path.removeprefix('/')}",
            headers=self.request_headers,
            **kwargs,
        )

        # handle non 2xx responses
        if not 200 <= response.status_code < 300:  # noqa: PLR2004
            message = (
                f"Quickbase API error - status {response.status_code}, "
                f"content: {response.text}"
            )
            raise RequestException(message)

        data = response.json()
        if cache and self.cache_results:
            self._cache[request_hash] = data

        return data

    def get_app_info(self) -> dict:
        """Retrieve information about the QB app.

        https://developer.quickbase.com/operation/getApp
        """
        return self.make_request(requests.get, f"apps/{self.app_id}")

    def get_table(self, table_id: str) -> dict:
        """Get single Tables as dictionary.

        https://developer.quickbase.com/operation/getTable
        """
        return self.make_request(requests.get, f"tables/{table_id}?appId={self.app_id}")

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
        return self.make_request(
            requests.post, "records", cache=False, json=upsert_payload
        )

    @staticmethod
    def parse_upsert_results(api_response: dict) -> dict | None:
        """Parse counts of modified records and errors from upsert."""
        metadata = api_response.get("metadata")
        if not metadata:
            return None

        results = {
            "processed": metadata.get("totalNumberOfRecordsProcessed", 0),
            "created": len(metadata.get("createdRecordIds", [])),
            "updated": len(metadata.get("updatedRecordIds", [])),
            "unchanged": len(metadata.get("unchangedRecordIds", [])),
            "errors": None,
        }

        if api_errors := metadata.get("lineErrors"):
            api_error_counts: dict[str, int] = defaultdict(int)
            for errors in api_errors.values():
                for error in errors:
                    api_error_counts[error] += 1
            results["errors"] = api_error_counts

        return results

    def prepare_upsert_payload(
        self,
        table_id: str,
        records: list[dict],
        merge_field: str | None = None,
        return_fields: bool = False,  # noqa: FBT001, FBT002
    ) -> dict:
        """Prepare an API payload for upsert.

        https://developer.quickbase.com/operation/upsert

        This method expects a list of dictionaries, one dictionary per record, with a
        {Field Label:Value} structure.  This method will first retrieve a mapping of
        Field label-to-ID mapping, then remap the data to a {Field ID:{value:Value}}
        structure.

        Then, return a dictionary payload suitable for the QB upsert API call.
        """
        field_map = self.get_table_fields_label_to_id(table_id)
        mapped_records = self.map_and_format_records_for_upsert(field_map, records)

        upsert_payload = {
            "to": table_id,
            "data": mapped_records,
            "fieldsToReturn": list(field_map.values()) if return_fields else [],
        }
        if merge_field:
            upsert_payload["mergeFieldId"] = field_map[merge_field]

        return upsert_payload

    def map_and_format_records_for_upsert(
        self,
        field_map: dict,
        records: list[dict],
    ) -> list[dict]:
        """Format list of {Field Label:Value} records into {Field ID:{value:Value}}."""
        mapped_records = []
        for record in records:
            mapped_record = {}
            for field_label, field_value in record.items():
                if field_id := field_map.get(field_label):
                    mapped_record[str(field_id)] = {"value": field_value}
                else:
                    message = f"Field label '{field_label}' not found in Field mappings."
                    raise QBFieldNotFoundError(message)
            mapped_records.append(mapped_record)
        return mapped_records

    def query_records(self, query: dict) -> dict:
        """Query for Table Records.

        https://developer.quickbase.com/operation/runQuery
        """
        return self.make_request(requests.post, "records/query", cache=False, json=query)

    def query_records_mapped_fields_iter(self, query: dict) -> Iterator[dict]:
        """Query for records, yielding records with fields mapped to their labels."""
        response = self.query_records(query)
        field_map = {f["id"]: f["label"] for f in response["fields"]}
        for record in response["data"]:
            yield {
                field_map[int(field_id)]: field["value"]
                for field_id, field in record.items()
            }

    def get_table_as_df(
        self,
        table_id: str,
        fields: list | None = None,
    ) -> pd.DataFrame:
        """Retrieve all records for a table as a DataFrame.

        If arg 'fields' if passed, results will be limited to only those fields from the
        table.

        Additionally, by relying on self.query_records_mapped_fields_iter() to iteratively
        yield records, this method is safe for large Quickbase tables.
        """
        table_fields_df = self.get_table_fields(table_id)
        if fields:
            table_fields_df = table_fields_df[table_fields_df.label.isin(fields)]

        records = self.query_records_mapped_fields_iter(
            {
                "from": table_id,
                "select": list(table_fields_df.id),
            }
        )

        return pd.DataFrame(
            records,
            columns=table_fields_df.label,
        )

    def delete_records(self, table_id: str, where_clause: str) -> dict:
        """Deleted Records from a Table given a where clause.

        https://developer.quickbase.com/operation/deleteRecords
        """
        return self.make_request(
            requests.delete,
            "records",
            cache=False,
            json={
                "from": table_id,
                "where": where_clause,
            },
        )

    def delete_all_table_records(self, table_id: str) -> dict:
        """Delete all records from a Table.

        This is accomplished by retrieving table fields, identifying the 'Record ID#'
        field ID, and then creating a query that deletes all records where record id is
        greater than 0 (this is the suggested method for truncating a QB table).
        """
        table = self.get_table(table_id)
        key_field_id = table["keyFieldId"]
        return self.delete_records(table_id, f"{{{key_field_id}.GT.0}}")
