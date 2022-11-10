from airflow.hooks.postgres_hook import PostgresHook
import os
import re
import requests
from scripts.paths import BASE_REPL_URL, DATA_DIR
import yaml

def get_sequence_range(postgres_conn_id, overpass_paths_list):
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
    sequence_range = [last_db_sequence, str(last_server_sequence)]

    return overpass_paths_list + sequence_range

def insert_last_repl_sequence(postgres_conn_id, **kwargs):
    repl_files = os.listdir(DATA_DIR + '/repl/')
    last_sequence = int(max(repl_files).rstrip('.osm.gz'))
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.insert_rows(
        'state."sequences"',
        [(last_sequence, kwargs['ds'])],
        ['sequence', 'last_run'],
        commit_every=0
    )