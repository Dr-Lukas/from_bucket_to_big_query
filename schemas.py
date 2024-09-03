from google.cloud import bigquery

SCHEMAS = {}
SCHEMAS["test"] = [ 
    bigquery.SchemaField('id', 'STRING'),
    bigquery.SchemaField('table_name', 'STRING'),
    bigquery.SchemaField('name', 'STRING'),
    bigquery.SchemaField('status', 'STRING'),
    bigquery.SchemaField('execution_time_in_s', 'FLOAT'),
    bigquery.SchemaField('message', 'STRING'),
    bigquery.SchemaField('failed_rows', 'INTEGER'),
    bigquery.SchemaField('bytes_processed', 'INTEGER'),
    bigquery.SchemaField('bytes_billed', 'INTEGER'),
    bigquery.SchemaField('job_location', 'STRING'),
    bigquery.SchemaField('job_project_id', 'STRING'),
    bigquery.SchemaField('slot_ms', 'INTEGER'),
    bigquery.SchemaField('price', 'FLOAT'),
    bigquery.SchemaField('started_at', 'TIMESTAMP'),
    bigquery.SchemaField('completed_at', 'TIMESTAMP')
]

SCHEMAS["freshness"]  = [
    bigquery.SchemaField('id', 'STRING'),
    bigquery.SchemaField('project_name', 'STRING'),
    bigquery.SchemaField('schema_name', 'STRING'),
    bigquery.SchemaField('table_name', 'STRING'),
    bigquery.SchemaField('latest_loaded_at', 'TIMESTAMP'),
    bigquery.SchemaField('queried_at', 'TIMESTAMP'),
    bigquery.SchemaField('time_since_last_row_arrived_in_s', 'FLOAT'),
    bigquery.SchemaField('status', 'STRING'),
    bigquery.SchemaField('filter_field', 'STRING'),
    bigquery.SchemaField('filter_type', 'STRING'),
    bigquery.SchemaField('filter_value', 'INTEGER'),
    bigquery.SchemaField('warn_after_period', 'STRING'),
    bigquery.SchemaField('warn_after_value', 'INTEGER'),
    bigquery.SchemaField('error_after_period', 'STRING'),
    bigquery.SchemaField('error_after_value', 'INTEGER'),
    bigquery.SchemaField('bytes_processed', 'INTEGER'),
    bigquery.SchemaField('bytes_billed', 'INTEGER'),
    bigquery.SchemaField('job_location', 'STRING'),
    bigquery.SchemaField('job_project_id', 'STRING'),
    bigquery.SchemaField('slot_ms', 'INTEGER'),
    bigquery.SchemaField('price', 'FLOAT'),
    bigquery.SchemaField('started_at', 'TIMESTAMP'),
    bigquery.SchemaField('completed_at', 'TIMESTAMP')
]