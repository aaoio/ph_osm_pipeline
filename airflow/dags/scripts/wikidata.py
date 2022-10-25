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
        r_query: 'regions',
        p_query: 'provinces',
        cm_query: 'cities_municipalities'
    }

    json_path = os.path.join(
        download_dir_now,
        f'{now.year}_{now.month}_{now.day}_{l_names[query]}_'+
        'wikidata.json'
    )

    with open(json_path, 'w') as f:
        json.dump(data, f)

def extract_wikidata(**kwargs):

    datetime_now = datetime.strptime(
        kwargs['ds'],
        '%Y-%m-%d'
    )
    download_dir_now = os.path.join(
        DATA_DIR,
        'wikidata',
        f'{datetime_now.year}_{datetime_now.month}_{datetime_now.day}'
    )

    queries = [r_query, p_query, cm_query]

    for query in queries:
        query_wikidata(query, download_dir_now, datetime_now)
