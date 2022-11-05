import os

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', '..'))
WEBDRIVER_PATH = '/usr/local/bin/geckodriver'
DATA_DIR = os.path.join(ROOT_DIR, 'data')

export_XPath = '//button[@data-ide-handler="click:onExportClick"]'
geojson_XPath = '//p[@id="export-geoJSON"]/div[@class="field-body"]/span/a[@title="saves the exported data as a file"]'
continue_anyway_XPath = '//button[text()="continue anyway"]'

BASE_REPL_URL = "https://planet.openstreetmap.org/replication/changesets/"