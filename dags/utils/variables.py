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

DATETIME = datetime.now().strftime('%Y-%m-%d')
AGG_TABLES = ['stations', 'networks', 'bike_activity']
INITIAL_TABLES = ['initial_network_stations']
ICEBERG_JARS_PATH = ["s3://citybikes-raw-data/jars/iceberg-spark3-runtime-0.13.2.jar"]