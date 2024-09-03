In this repository you will find everything you need to create and upload data to two tables in BigQuery. 
  - requirements.txt has all the aditional packages you need to run the code.
  - schemas.py describes two tables in BigQuery.
  - bigquery_connections.py defines a class BigQueryTable which contains functions for:
    - table_exists()- checks if the table that was created exist;
    - create_table()- creates required table and sets collumns which will be partitioned and clustered;
    - upload_data()- uploads data type of dictionary into the required location.
  - upload_data.py defined a class StorageData which contains functions for:
    - get_blob_contents() which returns a list of files inside of set bucket and folder;
    - create_unique_id() to create unique ids for each row to be uploaded by filename and job_id;
    - get_json_data() creates batched dictionaries of data to be inserted into the final directory.
  - config.yaml contains the code inputs, where:
    - bq_project_id- the BigQuery Project Id
    - bq_dataset_id- the dataset name of a given project
    - bq_table_id- the table name where the final data should be stored
    - bq_schema_name- which table will be uploaded and which schema needs to be selected from schemas.py
    - bucket_project- the project Id of buckets from where the initial data is coming from 
    - bucket_name- bucket name for selected bucket_project
    - bucket_folder_name- folder name from selected bucket
    - price- price of 1 TiB

How to run:
1. Setup:
    - pip install -r requirements.txt
    - setup connection with GCP
    - modify config.yaml
2. Create table:
    - python upload_data.py --create-table
3. Upload data:
    - python upload_data.py --upload-data
