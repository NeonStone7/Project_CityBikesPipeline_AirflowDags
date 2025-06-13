from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
    EmrServerlessStartJobOperator
)
from airflow.providers.amazon.aws.sensors.emr import (
    EmrStepSensor,
    EmrJobFlowSensor,
    EmrServerlessJobSensor
)
from airflow.utils.trigger_rule import TriggerRule

from airflow.decorators import dag, task, task_group
from utils.variables import (
    default_args,
    TRANSFORMED_BUCKET,
    ICEBERG_JARS_PATH,
    INITIAL_TABLES,
    NETWORK_IDS,
    RAW_BUCKET_NAME,
    DATETIME
)
from utils.emr import CLUSTER_CONFIG
from utils.commons import (
    build_serverless_spark_command,
    return_default_tasks
)
S3_JOBFILE_LOCATION = 's3://citybikes-raw-data/scripts/aggregation_job.py'
SPARK_CONFIG_OVERRIDES = {
    'monitoringConfiguration': {'s3MonitoringConfiguration':{'logUri':'s3://citybikes-raw-data/logs/'}}
}

@dag(
    dag_id = 'aggregated_transformer',
    default_args = default_args,
    schedule_interval = "0 6 * * *",
    catchup = False
) 
def transform_data():

    @task_group(group_id='processors')
    def process_data():
        for network in NETWORK_IDS:
            for table in INITIAL_TABLES:

                task_id = f'process_{network}_{table}'
                job_args = {
                    '--table_name': table,
                    '--source_bucket': RAW_BUCKET_NAME,
                    '--date': DATETIME,
                    '--network_name': network,
                    '--destination_bucket': TRANSFORMED_BUCKET,
                }

                job = EmrServerlessStartJobOperator(
                    task_id = task_id,
                    application_id=APPLICATION_ID,
                    execution_role_arn=ROLE_ARN,
                    job_driver = build_serverless_spark_command(S3_JOBFILE_LOCATION, ["s3://citybikes-raw-data/scripts/spark_jobs.zip"], ICEBERG_JARS_PATH, job_args),
                    configuration_overrides=SPARK_CONFIG_OVERRIDES,
                    name = f'{task_id}-{DATETIME}'
                )

                wait_step = EmrServerlessJobSensor(
                        task_id=f"wait_{task_id}",
                        application_id=APPLICATION_ID,
                        mode = 'reschedule',
                        poke_interval=30,
                    )
                job >> wait_step



    start, end = return_default_tasks()
    start >> process_data() >> end

transform_data()