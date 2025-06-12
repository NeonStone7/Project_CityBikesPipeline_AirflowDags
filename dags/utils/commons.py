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

    network_name = network_id.split('/')[-1].replace('-', '_').strip()

    filepath = f'dags/resources/json_output_{network_name}.json'

    with open(filepath, 'w') as file:
        json_object = json.dumps(return_json, indent = 4)
        file.write(json_object)
    
    return filepath, network_name


def return_default_tasks():

    start_task = EmptyOperator(task_id = 'start')
    end_task = EmptyOperator(task_id = 'end')
    return start_task, end_task