from scripts.paths import *
from scripts.queries.sparql_queries import *
from datetime import datetime
import json
import os
import pandas as pd
import requests

url = 'https://query.wikidata.org/sparql'

def query_wikidata(query, download_dir_now, now):
    """
    Download JSON file of data on Philippine regions, provinces, and
    cities/municipalities from wikidata
    """
    r = requests.get(url, params={'format': 'json', 'query': query})
    data = r.json()

    l_names = {
        r_query: ['3', 'regions'],
        p_query: ['4', 'provinces'],
        cm_query: ['6', 'cities_municipalities']
    }

    json_path = os.path.join(
        download_dir_now,
        f'{now.strftime("%Y")}_'+
        f'{now.strftime("%m")}_'+
        f'{now.strftime("%d")}_l{l_names[query][0]}_'+
        f'{l_names[query][1]}_'
        'wikidata.json'
    )

    with open(json_path, 'w') as f:
        json.dump(data, f)
    
    return json_path

def extract_wikidata(**kwargs):

    datetime_now = datetime.strptime(
        kwargs['ds'],
        '%Y-%m-%d'
    )
    download_dir_now = os.path.join(
        DATA_DIR,
        'wikidata',
        f'{datetime_now.strftime("%Y")}_'+
        f'{datetime_now.strftime("%m")}_'+
        f'{datetime_now.strftime("%d")}'
    )

    queries = [r_query, p_query, cm_query]

    filepaths = []
    for query in queries:
        filepath = query_wikidata(query, download_dir_now, datetime_now)
        filepaths.append(filepath)
    
    return filepaths

