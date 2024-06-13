from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict

key_path = 'bigquery_key.json'
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

project_id = credentials.project_id
dataset_id = 'saras_analytics'


def create_dataset():
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    try:
        dataset = client.create_dataset(dataset)  # API request
        print(f"Created dataset {client.project}.{dataset.dataset_id}")
    except Conflict:
        print(f"Dataset {client.project}.{dataset.dataset_id} Already Exists.")


def load_table_in_dataset(file_path, table_id):
    # Define the table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Define the job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    # Load the file into BigQuery
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for the job to complete
    print(f"Uploaded {file_path} to {table_ref}")


def bq_process():
    create_dataset()
    load_table_in_dataset('pq/snapshot_info.pq','snapshot_info')
    load_table_in_dataset('pq/products_df.pq','products')
    load_table_in_dataset('pq/vendors_df.pq','vendors')
    load_table_in_dataset('pq/warehouse_products_df.pq','warehouse_products')
    load_table_in_dataset('pq/item_bins_df.pq','item_bins')
