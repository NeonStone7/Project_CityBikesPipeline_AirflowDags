from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import (
    EmrStepSensor,
    EmrJobFlowSensor
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
    build_spark_submit_command,
    return_default_tasks
)
@dag(
    dag_id = 'initial_transformer',
    default_args = default_args,
    schedule_interval = "0 4 * * *",
    catchup = False
) 
def transform_data():

    # create cluster
    create_cluster = EmrCreateJobFlowOperator(
        task_id = 'create_cluster',
        job_flow_overrides=CLUSTER_CONFIG,
    )

    job_flow_id = create_cluster.output

    emr_cluster_ready = EmrJobFlowSensor(
        task_id = 'emr_cluster_ready',
        job_flow_id=job_flow_id,
        target_states = ['WAITING', 'RUNNING'],
    )

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

                add_step = EmrAddStepsOperator(
                    task_id = task_id,
                    job_flow_id=job_flow_id,
                    steps = build_spark_submit_command(task_id, 
                                                    "s3://citybikes-raw-data/scripts/transformation_job.py",
                                                    ["s3://citybikes-raw-data/scripts/spark_jobs.zip"], 
                                                    ICEBERG_JARS_PATH, 
                                                    job_args)
                )

                wait_step = EmrStepSensor(
                        task_id=f"wait_{task_id}",
                        job_flow_id=job_flow_id,
                        step_id="{{{{ task_instance.xcom_pull(task_ids='" + task_id + "', key='return_value')[0] }}}}",
                        poke_interval=30,
                    )
                add_step >> wait_step

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id = 'terminate_cluster',
        job_flow_id=job_flow_id,
        trigger_rule=TriggerRule.ALL_DONE,

    )

    start, end = return_default_tasks()
    start >> create_cluster >> emr_cluster_ready >> process_data() >> terminate_cluster >> end

transform_data()