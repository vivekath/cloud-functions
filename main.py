from google.cloud import bigquery
import os

DATASET_ID = os.environ.get("dataset_id")

def load_data_bigquery(cloud_event):
    try:
        event_data = cloud_event.data
        print(event_data)

        bucket_name = event_data["bucket"]
        file_name = event_data["name"]

        # Skip folders
        if file_name.endswith("/"):
            return "Skipping folder"

        source_uri = f"gs://{bucket_name}/{file_name}"

        table_name = file_name.split(".")[0] + "Table"
        table_id = f"{DATASET_ID}.{table_name}"

        client = bigquery.Client()

        file_extension = file_name.split(".")[-1].lower()

        if file_extension == "csv":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
            )

        elif file_extension == "json":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
            )

        elif file_extension == "parquet":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                autodetect=True,
            )
        else:
            return f"Unsupported file type: {file_extension}"

        load_job = client.load_table_from_uri(
            source_uri,
            table_id,
            job_config=job_config,
        )

        load_job.result()

        return f"Loaded {file_name} into {table_id}"

    except Exception as e:
        print(e)
        return str(e)
    
"""
{
  'kind': 'storage#object',
  'id': 'src-bkt-17122025/transactions.csv/1767690877811047',
  'selfLink': 'https://www.googleapis.com/storage/v1/b/src-bkt-17122025/o/transactions.csv',
  'name': 'transactions.csv',
  'bucket': 'src-bkt-17122025',
  'generation': '1767690877811047',
  'metageneration': '1',
  'contentType': 'text/csv',
  'timeCreated': '2026-01-06T09:14:37.816Z',
  'updated': '2026-01-06T09:14:37.816Z',
  'storageClass': 'STANDARD',
  'timeStorageClassUpdated': '2026-01-06T09:14:37.816Z',
  'size': '296',
  'md5Hash': 'elUTMjRVo+NoLXFBoQrRow==',
  'mediaLink': 'https://storage.googleapis.com/download/storage/v1/b/src-bkt-17122025/o/transactions.csv?generation=1767690877811047&alt=media',
  'crc32c': 'tiKV5g==',
  'etag': 'COfy4NbJ9pEDEAE='
}
"""