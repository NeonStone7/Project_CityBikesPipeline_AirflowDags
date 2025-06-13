import requests
from dotenv import load_dotenv
import json
from airflow.operators.empty import EmptyOperator

load_dotenv()

def retrieve_and_save_json(network_id):

    try: 
        return_json = requests.get(network_id).json()

    except Exception as e:
        print(f'Error fetching data for {network_id}: {e}')
        return None

    network_name = network_id.split('/')[-1].strip()

    filepath = f'dags/resources/json_output_{network_name}.json'

    with open(filepath, 'w') as file:
        json_object = json.dumps(return_json, indent = 4)
        file.write(json_object)
    
    return filepath, network_name


def return_default_tasks():

    start_task = EmptyOperator(task_id = 'start')
    end_task = EmptyOperator(task_id = 'end')
    return start_task, end_task

def spark_cmd(task_id, args):
    return [
        {
                'Name': task_id,
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': args
                }
            }
        ]

def build_spark_submit_command(task_id, job_file, py_files, jars_path, job_args):
    start = ['spark-submit',
         '--deploy-mode', 
         'cluster',
         '--master',
         'yarn']

    start.extend([f"--jars", ','.join(jars_path)])
    start.extend([f"--py-files", ','.join(py_files)])

    start.append(job_file)

    
    for key, value in job_args.items():
        start.append(key)
        start.append(value)

    return spark_cmd(task_id, start)

def build_serverless_spark_command(S3_JOBFILE_LOCATION, pyfiles, jars, job_args):
    # build spark submit for emr serverless
    job_driver = {
        "sparkSubmit": {
            "sparkSubmitParameters":
                    {
                    "entryPoint": S3_JOBFILE_LOCATION,
                    "entryPointArguments": [],
                    "sparkSubmitParameters": 
                        "--conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog "
                        + "--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog "
                        + "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                        + "--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog "
                        + "--conf spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                        + "--conf spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                        + "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory "
                        + "--conf spark.hadoop.proxyuser.hive.hosts=* " 
                        + "--conf spark.hadoop.proxyuser.hive.groups=* "
                        + "--conf spark.hadoop.f3.s3a.endpoint.region=eu-west-1 ",
                        }}}


    job_driver['sparkSubmit']['sparkSubmitParameters']['sparkSubmitParameters'] = job_driver['sparkSubmit']['sparkSubmitParameters']['sparkSubmitParameters'] + '--py-files ' + ','.join(pyfiles) + ' '
    job_driver['sparkSubmit']['sparkSubmitParameters']['sparkSubmitParameters'] = job_driver['sparkSubmit']['sparkSubmitParameters']['sparkSubmitParameters'] + '--jars ' + ','.join(jars) + ' '

    for key,value in job_args.items():
        job_driver['sparkSubmit']['sparkSubmitParameters']['entryPointArguments'].extend([key, value])

    return job_driver
