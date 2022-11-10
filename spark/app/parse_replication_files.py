import json
import geopandas as gpd
import os
import pyspark
from pyspark.sql import SparkSession, types, functions
import requests
from shapely.geometry import shape, Point
import sys

REPL = '/usr/local/data/raw/repl/'

# Overpass geojson paths
overpass_r_path = sys.argv[1]
overpass_p_path = sys.argv[2]
overpass_cm_path = sys.argv[3]
overpass_ph_path = sys.argv[4]

# Sequence range
last_db_sequence = int(sys.argv[5])
last_server_sequence = int(sys.argv[6])

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
    download_path = os.path.join(REPL, f'{sequence}.osm.gz')
    
    # Download file
    if not os.path.exists(download_path):
        try:
            replicationFile = requests.get(fileURL, stream=True)
            replicationData = replicationFile.raw
            with open(download_path, 'wb') as f:
                f.write(replicationData.read())
        except Exception as e:
                print(e)

def create_geodf(geojson_path):
    geodf = gpd.GeoDataFrame.from_file(geojson_path)
    mask = (
        (geodf['type']=='boundary') &
        (geodf['@id'].str.startswith('relation/') &
        (geodf['name'].notnull()))
    )
    geodf = geodf[mask]
    geodf['@id'] = geodf['@id'].map(lambda x: x[9:])
    geodf = geodf [['@id', 'geometry']]
    return geodf

def find_containing_relation(centroid, geodf):
    centroid_point = Point(centroid)
    for relation, geom in geodf.itertuples(index=False):
        if geom.contains(centroid_point):
            return int(relation)

def main():
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Download replication files
    sequences_df = spark.range(last_db_sequence, last_server_sequence)
    sequences_df.foreach(lambda x: download_replication_file(x['id']))

    # Read replication files
    df = spark.read.format('xml') \
        .options(rowTag='changeset') \
        .load(REPL)\
        .drop('discussion', 'tag')\

    df = df.dropna(subset=[
        '_id',
        '_uid',
        '_user',
        '_created_at',
        '_closed_at',
        '_num_changes',
        '_max_lat',
        '_max_lon',
        '_min_lat',
        '_min_lon',
        '_open'
    ])

    # Find centroid coordinates
    mean_udf = functions.udf(lambda array: sum(array)/len(array),
        returnType=types.DoubleType())

    df = df \
        .withColumn('centroid_lon',
            mean_udf(functions.array(df['_min_lon'], df['_max_lon']))) \
        .withColumn('centroid_lat',
            mean_udf(functions.array(df['_min_lat'], df['_max_lat'])))

    # Exclude open changesets and changesets with centroid
    # outside PH bounds
    with open(overpass_ph_path, 'r') as f:
        overpass_ph = json.load(f)
    shape_ph = shape(overpass_ph)
    bounds = shape_ph.bounds
    bounds_max_lat = bounds[3]
    bounds_max_lon = bounds[2]
    bounds_min_lat = bounds[1]
    bounds_min_lon = bounds[0]

    df = df.filter(
        (df['centroid_lon']>=bounds_min_lon) &
        (df['centroid_lon']<=bounds_max_lon) &
        (df['centroid_lat']>=bounds_min_lat) &
        (df['centroid_lat']<=bounds_max_lat) &
        (df['_open']==False)
    )

    # Filter using geojson geometry
    in_ph = functions.udf(lambda x: shape_ph.contains(Point(x)))
    df = df\
        .withColumn('in_ph',
            in_ph(functions.array(
                df['centroid_lon'],
                df['centroid_lat']
            ))
        )

    df = df.filter(df['in_ph']==True)

    # Identify region, province, and city/municipality
    r_gdf = create_geodf(overpass_r_path)
    p_gdf = create_geodf(overpass_p_path)
    cm_gdf = create_geodf(overpass_cm_path)

    find_containing_region = functions.udf(
        lambda x: find_containing_relation(x, r_gdf),
        returnType=types.IntegerType()
    )
    find_containing_province = functions.udf(
        lambda x: find_containing_relation(x, p_gdf),
        returnType=types.IntegerType()
    )
    find_containing_city_municipality = functions.udf(
        lambda x: find_containing_relation(x, cm_gdf),
        returnType=types.IntegerType()
    )

    df = df \
        .withColumn(
            'region_relation_id',
            find_containing_region(functions.array(
                df['centroid_lon'], df['centroid_lat']
            ))
        ) \
        .withColumn(
            'province_relation_id',
            find_containing_province(functions.array(
                df['centroid_lon'], df['centroid_lat']
            ))
        ) \
        .withColumn(
            'city_municipality_relation_id',
            find_containing_city_municipality(functions.array(
                df['centroid_lon'], df['centroid_lat']
            ))
        )

    repl_df = df.select(
        (df._closed_at.cast(types.TimestampType())\
            .alias('closed_at')),
        (df._comments_count.cast(types.IntegerType())\
            .alias('comments_count')),
        (df._created_at.cast(types.TimestampType())\
            .alias('created_at')),
        (df._id.cast(types.IntegerType())\
            .alias('id')),
        (df._max_lat.cast(types.DoubleType())\
            .alias('max_lat')),
        (df._max_lon.cast(types.DoubleType())\
            .alias('max_lon')),
        (df._min_lat.cast(types.DoubleType())\
            .alias('min_lat')),
        (df._min_lon.cast(types.DoubleType())\
            .alias('min_lon')),
        'centroid_lat',
        'centroid_lon',
        (df._num_changes.cast(types.IntegerType())\
            .alias('num_changes')),
        (df._uid.cast(types.IntegerType())\
            .alias('uid')),
        (df._user.cast(types.StringType())\
            .alias('username')),
        'region_relation_id',
        'province_relation_id',
        'city_municipality_relation_id'
    )

    repl_df.write \
        .mode('append') \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://docker_database_1:5432/osm") \
        .option("dbtable", "staging.stg_osm_changesets") \
        .option("user", "osm") \
        .option("password", "osm") \
        .option("driver", "org.postgresql.Driver") \
        .save()

if __name__=='__main__':
    main()