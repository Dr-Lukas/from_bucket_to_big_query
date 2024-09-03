import json
import hashlib
import argparse
import yaml
import re
from typing import Generator, Tuple, Dict, Any, List
from google.cloud import storage
from bigquery_connections import BigQueryTable
from schemas import SCHEMAS


class StorageData:
    def __init__(
        self,
        project: str,
        bucket_name: str,
        folder_name: str,
        price: float,
        batch_size=1000,
    ):
        self.folder_name = folder_name
        client = storage.Client(project=project)
        self.bucket = storage.Bucket(client, name=bucket_name)
        # calculating price of one Byte
        self.byte_price = price / 137_438_953_472
        self.batch_size = batch_size

    def get_blob_contents(self) -> Generator[Tuple[Dict[str, Any], str], None, None]:
        blobs = self.bucket.list_blobs(prefix=self.folder_name)
        for blob in blobs:
            data = blob.download_as_text()
            filename = "/".join(blob.name.split("/")[1:])
            try:
                data_dict = json.loads(data)
                yield data_dict, filename
            except json.decoder.JSONDecodeError:
                pass

    @staticmethod
    def create_unique_id(filename: str, job_id: str) -> str:
        # Create an MD5 hash for the ID
        string_to_hash = f"{filename}{job_id}"
        md5_hash = hashlib.md5()
        md5_hash.update(string_to_hash.encode("utf-8"))
        return md5_hash.hexdigest()

    def get_json_data(
        self, schema_name: str
    ) -> Generator[List[Dict[str, any]], None, None]:
        # Define a list to hold the data
        data_to_insert = []

        for row, filename in self.get_blob_contents():
            results = row["results"]
            metadata = row["metadata"]

            for result in results:
                adapter_response = result["adapter_response"]
                timing = result["timing"]

                unique_id = self.create_unique_id(filename, adapter_response["job_id"])

                if schema_name == "test":
                    my_dictionary = {
                        "id": unique_id,
                        "table_name": "test",
                        "name": filename,
                        "status": result["status"],
                        "execution_time_in_s": result["execution_time"],
                        "message": adapter_response["_message"],
                        "failed_rows": result["failures"],
                        "bytes_processed": adapter_response["bytes_processed"],
                        "bytes_billed": adapter_response["bytes_billed"],
                        "job_location": adapter_response["location"],
                        "job_project_id": adapter_response["job_id"],
                        "slot_ms": adapter_response.get("slot_ms"),
                        "price": adapter_response["bytes_billed"] * self.byte_price,
                        "started_at": timing[0]["started_at"],
                        "completed_at": timing[1]["completed_at"],
                    }

                elif schema_name == "freshness":
                    criteria = result["criteria"]
                    filter = criteria["filter"]
                    if filter is not None:
                        filter_type_match = re.search(r"[<>=!]+", filter)
                        filter_split = re.split(r"[<>=!]+", filter)
                        filter_type = filter_type_match.group()
                        filter_field = filter_split[0].strip()
                        filter_value = filter_split[1].strip()

                    else:
                        filter_type = None
                        filter_field = None
                        filter_value = None

                    my_dictionary = {
                        "id": unique_id,
                        "project_name": adapter_response["project_id"],
                        "schema_name": "freshness",
                        "table_name": filename,
                        "latest_loaded_at": result["max_loaded_at"],
                        "queried_at": metadata["generated_at"],
                        "time_since_last_row_arrived_in_s": result[
                            "max_loaded_at_time_ago_in_s"
                        ],
                        "status": result["status"],
                        "filter_field": filter_field,
                        "filter_type": filter_type,
                        "filter_value": filter_value,
                        "warn_after_period": criteria["warn_after"]["period"],
                        "warn_after_value": criteria["warn_after"]["count"],
                        "error_after_period": criteria["error_after"]["period"],
                        "error_after_value": criteria["error_after"]["count"],
                        "bytes_processed": adapter_response["bytes_processed"],
                        "bytes_billed": adapter_response["bytes_billed"],
                        "job_location": adapter_response["location"],
                        "job_project_id": adapter_response["job_id"],
                        "slot_ms": adapter_response.get("slot_ms"),
                        "price": adapter_response["bytes_billed"] * self.byte_price,
                        "started_at": result["timing"][0]["started_at"],
                        "completed_at": result["timing"][1]["completed_at"],
                    }
                else:
                    print(f"dictionary not setup for {schema_name}")

                data_to_insert.append(my_dictionary)

            if len(data_to_insert) >= self.batch_size:
                yield data_to_insert
                data_to_insert = []

        if data_to_insert:
            yield data_to_insert


def main(parser):
    with open("config.yaml", "r") as yaml_file:
        config = yaml.safe_load(yaml_file)

    bq_project_id = config["bq_project_id"]
    bq_dataset_id = config["bq_dataset_id"]
    bq_table_id = config["bq_table_id"]
    bq_schema_name = config["bq_schema_name"]

    bucket_project = config["bucket_project"]
    bucket_name = config["bucket_name"]
    bucket_folder_name = config["bucket_folder_name"]
    price = config["price"]

    my_table = BigQueryTable(
        bq_project_id, bq_dataset_id, bq_table_id, SCHEMAS[bq_schema_name]
    )

    if parser.create_table:
        my_table.create_table(bq_schema_name)

    if parser.upload_data:
        my_data = StorageData(bucket_project, bucket_name, bucket_folder_name, price)
        for data in my_data.get_json_data(bq_schema_name):
            my_table.upload_data(data)
        print(f"All Data Uploaded")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute a BigQuery query.")
    parser.add_argument("--create-table", action="store_true")
    parser.add_argument("--upload-data", action="store_true")

    args = parser.parse_args()

    main(args)
