import os
import pyspark
from pyspark.sql import SparkSession
import requests
import sys

spark = (SparkSession
    .builder
    .getOrCreate()    
)

DATA_DIR = '/usr/local/data'

sequences = str(sys.argv[1]).split(' ')
last_db_sequence = int(sequences[0])
last_server_sequence = int(sequences[1])


def download_replication_file(sequence):
    # Construct file URL
    sequenceNumber = str(sequence).zfill(9)
    topdir = str(sequenceNumber)[:3]
    subdir = str(sequenceNumber)[3:6]
    fileNumber = str(sequenceNumber)[-3:]
    fileURL = 'https://planet.openstreetmap.org/replication/' \
    + 'changesets/' \
    + topdir \
    + '/' \
    + subdir \
    + '/' \
    + fileNumber \
    + '.osm.gz'
    
    # Construct file download path
    download_path = os.path.join(DATA_DIR, 'repl', f'{sequence}.osm.gz')
    
    # Download file
    if not os.path.exists(download_path):
        try:
            replicationFile = requests.get(fileURL, stream=True)
            replicationData = replicationFile.raw
            with open(download_path, 'wb') as f:
                f.write(replicationData.read())
        except Exception as e:
                print(e)

sequences_df = spark.range(last_db_sequence, last_server_sequence)
sequences_df.foreach(lambda x: download_replication_file(x['id']))