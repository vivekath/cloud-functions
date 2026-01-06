from google.cloud import bigquery 

import os

DATASET_ID = os.environ.get("dataset_id")
def load_data_bigquery(request):

    try:
        event_data = request.get_json()
        print(event_data)

        # get gcs details
        bucket_name = event_data['bucket']
        file_name = event_data['name'] # sales.csv
        source_uri = f'gs://{bucket_name}/{file_name}'

        # get bq details 
        dataset_id = DATASET_ID
        table_name = file_name.split('.')[0]+'Table' # salesTable
        table_id = f'{dataset_id}.{table_name}'

        # Construct a BigQuery client object.
        client = bigquery.Client()

        file_extension = file_name.split('.')[-1] #csv

        if file_extension == 'csv':
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
            )

        elif file_extension == 'json':
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
            )

        elif file_extension == 'parquet':
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                autodetect=True,
            )

        load_job = client.load_table_from_uri(
            source_uri, 
            table_id, 
            job_config=job_config
        )

        load_job.result()  # Waits for the job to complete.

        return f'successfully the loading job for {file_name} to {table_id}'

    except Exception as e:
        print(f"Error: {e}")
        return f"Error: {e}"
    
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