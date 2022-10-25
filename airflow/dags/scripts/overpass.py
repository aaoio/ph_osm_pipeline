from scripts.paths import *
from scripts.queries.overpass_queries import *
from datetime import datetime
import geojson
import json
import os
from OSMPythonTools.overpass import Overpass
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from urllib.parse import quote_plus

def get_national_geom(download_dir_now, now):
    """
    Return a multipolygon of Philippine national borders
    and dump to a GeoJSON file.
    Uses OSMPythonTools' Overpass API wrapper.
    """

    geojson_path = os.path.join(
        download_dir_now,
        f'{now.year}_{now.month}_{now.day}_l2_national_overpass.geojson'
    )

    overpass = Overpass()
    ph_overpass_result = overpass.query(ph_query)
    ph_geom = ph_overpass_result.elements()[0].geometry()
    
    with open(geojson_path, 'w') as f:
        geojson.dump(ph_geom, f)

    return ph_geom

def get_overpass_turbo_geojson(query, download_dir_now, now):
    """
    Download query results from overpass-turbo.eu in GeoJSON format.
    """
    
    # Set firefox download directory
    ff_options = webdriver.firefox.options.Options()
    ff_options.headless = True
    ff_options.set_preference('browser.download.folderList', 2)
    ff_options.set_preference('browser.download.dir', download_dir_now)
    
    # Build URL
    encoded_query = quote_plus(query)
    overpass_url = f'https://overpass-turbo.eu/?Q={encoded_query}&R'

    driver = webdriver.Firefox(
        executable_path=WEBDRIVER_PATH,
        options=ff_options
    )
    driver.get(overpass_url)

    # Sequence to download file
    try:
        continue_element = WebDriverWait(driver, 600).until(
            EC.presence_of_element_located(
                (By.XPATH, continue_anyway_XPath)
            )
        )
    finally:
        continue_element.click()

    time.sleep(5)
    export_element = driver.find_element(By.XPATH, export_XPath)
    export_element.click()

    try:
        geojson_element = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, geojson_XPath))
        )
    finally:
        geojson_element.click()
        time.sleep(12)
        driver.quit()

    # Rename the newest file
    ## Find newest file
    with os.scandir(download_dir_now) as it:
        dl_files = []
        for file in it:
            if file.name[:6]=='export' and file.name[-8:]=='.geojson':
                dl_files.append(file)
        newest_file = max(dl_files, key=lambda x:x.stat().st_mtime)
        newest_file_name = newest_file.name
    
    ## Check if newest file is what we just downloaded
    newest_file_path = os.path.join(download_dir_now, newest_file_name)
    with open(newest_file_path, 'r') as f:
        nf_json = json.load(f)
    
    time_of_download = datetime.strptime(
        nf_json['timestamp'],
        '%Y-%m-%dT%H:%M:%SZ'
    )
    time_since_download = now - time_of_download

    admin_level = nf_json['features'][0]['properties']['admin_level']
    query_admin_level = query[55]
    
    l_names = {
        '3': 'regional',
        '4': 'provincial',
        '6': 'city_municipality'
    }

    rename_to = f'{now.year}_{now.month}_{now.day}\
_l{admin_level}_{l_names[admin_level]}_overpass.geojson'

    if ((time_since_download.days <= 1) and
        (admin_level==query_admin_level)):
        os.rename(
            newest_file_path,
            os.path.join(
                download_dir_now,
                rename_to
            )
        )

def extract_overpass(**kwargs):
    
    datetime_now = datetime.strptime(
        kwargs['ds'],
        '%Y-%m-%d'
    )
    download_dir_now = os.path.join(
        DATA_DIR,
        'overpass',
        f'{datetime_now.year}_{datetime_now.month}_{datetime_now.day}'
    )

    queries = [r_query, p_query, cm_query]
    for q in queries:
        get_overpass_turbo_geojson(q, download_dir_now, datetime_now)
    
    get_national_geom(download_dir_now, datetime_now)