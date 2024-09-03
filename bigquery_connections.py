import time
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


class BigQueryTable:
    def __init__(self, project_id: str, dataset_id: str, table_id: str, schema: list):
        self.schema = schema
        self.project = project_id
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = self.client.dataset(dataset_id)
        self.table_ref = self.dataset_ref.table(table_id)
        if self.table_exists():
            self.table = self.client.get_table(self.table_ref)

    def table_exists(self) -> bool:
        try:
            self.client.get_table(self.table_ref)
            return True  # The table exists
        except NotFound:
            return False  # The table does not exist

    def create_table(self, schema_name: str):
        table = bigquery.Table(self.table_ref, schema=self.schema)

        if schema_name == "test":
            table.time_partitioning = bigquery.TimePartitioning(field="started_at")
            table.clustering_fields = ["failed_rows", "job_location"]
        elif schema_name == "freshness":
            table.time_partitioning = bigquery.TimePartitioning(field="queried_at")
            table.clustering_fields = ["status", "job_location"]
        else:
            print(f"schema not setup for: {schema_name}")

        self.client.create_table(table)
        # Wait for the table to be created and available before proceeding
        while not self.table_exists():
            time.sleep(1)

        print(f"Table {table.table_id} created.")

    def upload_data(self, data: dict):
        errors = self.client.insert_rows(
            self.table, data, selected_fields=self.table.schema
        )

        if not errors:
            print("Data chunk uploaded successfully.")
        else:
            print(f"Errors while uploading data chuck: {errors}, retrying...")
            for _ in range(3):
                errors = self.client.insert_rows(
                    self.table, data, selected_fields=self.table.schema
                )
                if not errors:
                    print("Data chunk uploaded successfully.")
                    break
                else:
                    print(f"Errors while uploading data chuck: {errors}")
                    time.sleep(1)
