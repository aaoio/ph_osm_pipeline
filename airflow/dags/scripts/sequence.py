from airflow.hooks.postgres_hook import PostgresHook
import re
import requests
from scripts.paths import BASE_REPL_URL
import yaml

def get_sequence_range(postgres_conn_id):
    # Last DB sequence
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    sql = 'SELECT MAX(sequence) FROM state."sequences"'
    sql_output = hook.get_first(sql)
    last_db_sequence = re.sub('[^0-9]', '', str(sql_output))

    # Last OSM replication server sequence
    server_state_path = BASE_REPL_URL + "state.yaml"
    state_yaml = yaml.load(
        requests.get(server_state_path).text,
        Loader=yaml.SafeLoader
    )
    last_server_sequence = state_yaml['sequence']

    return(f'{last_db_sequence} {last_server_sequence}')