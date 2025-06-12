import os
from airflow.decorators import dag, task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils.commons import (
    retrieve_and_save_json,
    return_default_tasks
)
from utils.variables import (
    default_args,
    AWS_CONN_ID,
    RAW_BUCKET_NAME,
    NETWORK_IDS,
    DATETIME
)

@dag(
    dag_id = 'ingestion_from_api',
    default_args = default_args,
    schedule_interval = "0 12 * * *"
)
def ingestion_from_api():

    start_task, end_task = return_default_tasks()

    filepaths = {}

    @task_group(group_id = 'api_calls')
    def make_api_calls():
        for network_id in NETWORK_IDS:

            @task(task_id = f'fetch_data_{network_id}')
            def fetch_data(network_id=network_id):

                filepath, network_name = retrieve_and_save_json(f'https://api.citybik.es/v2/networks/{network_id}')
                return {'filepath': filepath, 'network_name':network_name}

            @task(task_id = f'upload_data_{network_id}')
            def upload_to_s3(filepaths):

                network_name = filepaths['network_name']
                filepath = filepaths['filepath']

                s3hook = S3Hook(aws_conn_id = AWS_CONN_ID)
                s3hook.load_file(
                    filename = filepath,
                    key = f'{network_name}/{DATETIME}/{network_name}.json',
                    bucket_name = RAW_BUCKET_NAME,
                    replace = True,
                    gzip = False
                )

            filepaths = fetch_data()

            upload_to_s3(filepaths)
            
    start_task >> make_api_calls() >> end_task

ingestion_from_api()

