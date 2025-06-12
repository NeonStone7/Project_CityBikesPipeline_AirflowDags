from datetime import datetime, timedelta

# Default arguments for Airflow DAGs
default_args = {
    'owner':'oamen',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

AWS_CONN_ID = 'aws_default'
RAW_BUCKET_NAME = "citybikes-raw-data"
TRANSFORMED_BUCKET = "citybikes-transformed-data"
NETWORK_IDS = ["abu-dhabi-careem-bike", "acces-velo-saguenay"]

DATETIME = datetime.now().strftime('%Y-%M-%d')