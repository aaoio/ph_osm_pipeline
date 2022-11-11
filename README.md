# ph_osm_pipeline
Pipeline that extracts geographical data from Wikidata, geometric data from OpenStreetMap's (OSM) Overpass API, and [OSM change history metadata](https://planet.openstreetmap.org/replication/changesets/) for changes made to the Philippines. After filtering out changes outside of the Philippines, it identifies what region, province, and city was modified by each changeset. The end result is a data warehouse with a fact table for changesets and dimension tables for geographical data and users.

This is an iteration of a previous project. My goal in this project is to learn the basics of batch processing and workflow orchestration.

## How it works
1. Wikidata and the Overpass API is queried for data on Philippine administrative divisions. A GeoJSON of national borders is downloaded via [OSMPythonTools](https://github.com/mocnik-science/osm-python-tools)' Overpass API wrapper. Getting GeoJSON files for regions, provinces, and cities this way is much slower, so those are instead downloaded from [Overpass Turbo](https://overpass-turbo.eu/) using Selenium.
2. A pySpark script downloads and reads the Changeset XML files to a Spark dataframe. The GeoJSON files from the previous step are read to GeoPandas GeoDataFrames. The centroid coordinates of each changeset's [bounding box](https://wiki.openstreetmap.org/wiki/Bounding_Box) are calculated. The script checks if the centroid is in the Philippines, and then references the GeoDataFrames to identify what administrative divisions each changeset's centroid falls within.
3. Raw data from the Overpass and Wikidata GeoJSON/JSON files are cleaned and used to create slowly changing dimensions (type 2).
4. The ETL pipeline is scheduled in Airflow to run weekly. 

## How to use it
1. Create directory for the Postgres container bind mount.
```bash
mkdir data/db
```
2. Download the PostgreSQL JDBC driver to the `spark/resources` directory.
```bash
curl -L https://jdbc.postgresql.org/download/postgresql-42.5.0.jar -o ./spark/resources/postgresql-42.5.0.jar`
```
3. Start Airflow, Spark and Postgres containers.
```bash
cd docker
docker-compose airflow-init
docker-compose up -d
```
4. Before executing the workflow for the first time, set the initial changeset replication sequence number you would like the DAG to start working with (in this example, 5232300):
```bash
$ docker exec -it docker_database_1 psql -U osm -d osm
psql (15.0 (Debian 15.0-1.pgdg110+1))
Type "help" for help.

osm=# insert into state.sequences(sequence, last_run) values(5232300, null);
INSERT 0 1
```

## Notes
- It is possible for the centroid of a changeset's bounding box to be contained within the boundaries of a city or a province even though the bounding box itself spans an area larger than those boundaries.

### To add
- Data quality checks in Airflow
- Date dimension
