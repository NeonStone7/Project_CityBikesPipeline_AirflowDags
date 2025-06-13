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
from airflow.decorators import dag, task, task_group
from utils.variables import (
    default_args,
    TRANSFORMED_BUCKET,
    ICEBERG_JARS_PATH,
    AGG_TABLES,
    NETWORK_IDS
)
from utils.emr import CLUSTER_CONFIG
from utils.commons import (
    build_spark_submit_command,
    return_default_tasks
)
@dag(
    dag_id = 'transform_data_abdukareembikes',
    default_args = default_args,
    schedule_interval = "0 4 * * *",
) 
def transform_data():

    # create cluster
    create_cluster = EmrCreateJobFlowOperator(
        task_id = 'create_cluster',
        job_flow_overrides=CLUSTER_CONFIG,
    )

    job_flow_id = f"{{{{ task_instance.xcom_pull(task_ids='{create_cluster.task_id}', key='return_value') }}}}'"

    emr_cluster_ready = EmrJobFlowSensor(
        task_id = 'emr_cluster_ready',
        job_flow_id=job_flow_id,
        target_states = ['WAITING', 'RUNNING'],
    )

    @task_group(group_id='processors')
    def process_data():
        for network in NETWORK_IDS:
            for table in AGG_TABLES:
                task_id = f'process_{table}'
                add_step = EmrAddStepsOperator(
                    task_id = task_id,
                    job_flow_id=job_flow_id,
                    steps = build_spark_submit_command(task_id, 
                                                    job_file,
                                                    py_files, 
                                                    ICEBERG_JARS_PATH, 
                                                    job_args)

                )
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id = 'terminate_cluster',
        job_flow_id=job_flow_id,
    )

    start, end = return_default_tasks()
    start >> create_cluster >> emr_cluster_ready >> process_data() >> terminate_cluster >> end