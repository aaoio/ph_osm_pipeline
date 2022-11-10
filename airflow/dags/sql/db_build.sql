-- Create schema
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS changesets;
CREATE SCHEMA IF NOT EXISTS state;

-- Create tables
CREATE TABLE IF NOT EXISTS staging."stg_osm_changesets"(
    closed_at TIMESTAMP WITHOUT TIME ZONE,
    comments_count INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    id BIGINT,
    max_lat DOUBLE PRECISION,
    max_lon DOUBLE PRECISION,
    min_lat DOUBLE PRECISION,
    min_lon DOUBLE PRECISION,
    centroid_lat DOUBLE PRECISION,
    centroid_lon DOUBLE PRECISION,
    num_changes INTEGER,
    uid BIGINT,
    username VARCHAR,
    region_relation_id INTEGER,
    province_relation_id INTEGER,
    city_municipality_relation_id INTEGER
);

CREATE TABLE IF NOT EXISTS staging."stg_regions"(
    region_relation_id INTEGER NOT NULL PRIMARY KEY,
    region_name VARCHAR NOT NULL,
    region_wikidata_item INTEGER NOT NULL,
    region_population INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS staging."stg_provinces"(
    province_relation_id INTEGER NOT NULL PRIMARY KEY,
    province_name VARCHAR NOT NULL,
    province_wikidata_item INTEGER NOT NULL,
    province_population INTEGER NOT NULL,
    province_is_within VARCHAR,
    province_is_within_wikidata_item INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS staging."stg_cities_municipalities"(
    city_municipality_relation_id INTEGER NOT NULL PRIMARY KEY,
    city_municipality_name VARCHAR NOT NULL,
    city_municipality_wikidata_item INTEGER,
    city_municipality_type VARCHAR,
    city_municipality_income_class VARCHAR,               
    city_municipality_population INTEGER,
    city_municipality_is_within VARCHAR,
    city_municipality_is_within_wikidata_item INTEGER,
    city_municipality_area NUMERIC
);

CREATE TABLE IF NOT EXISTS state."sequences"(
    sequence BIGINT NOT NULL PRIMARY KEY,
    last_run DATE
);

CREATE TABLE IF NOT EXISTS changesets."users"(
    user_id BIGINT NOT NULL PRIMARY KEY,
    username VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS changesets."regions"(
    region_id SERIAL PRIMARY KEY,
    region_relation_id INTEGER NOT NULL,
    region_name VARCHAR NOT NULL,
    region_wikidata_item INTEGER NOT NULL,
    region_population INTEGER NOT NULL,
    start_date DATE,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS changesets."provinces"(
    province_id SERIAL PRIMARY KEY,
    province_relation_id INTEGER NOT NULL,
    province_name VARCHAR NOT NULL,
    province_wikidata_item INTEGER NOT NULL,
    province_population INTEGER NOT NULL,
    province_is_within VARCHAR,
    province_is_within_wikidata_item INTEGER NOT NULL,
    start_date DATE,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS changesets."cities_municipalities"(
    city_municipality_id SERIAL PRIMARY KEY,
    city_municipality_relation_id INTEGER NOT NULL,
    city_municipality_name VARCHAR NOT NULL,
    city_municipality_wikidata_item INTEGER,
    city_municipality_type VARCHAR,
    city_municipality_income_class VARCHAR,               
    city_municipality_population INTEGER,
    city_municipality_is_within VARCHAR,
    city_municipality_is_within_wikidata_item INTEGER,
    city_municipality_area NUMERIC,
    start_date DATE,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS changesets."osm_changesets"(
    changeset_id BIGINT,
    user_id BIGINT,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    closed_at TIMESTAMP WITHOUT TIME ZONE,
    num_changes INTEGER,
    max_lat NUMERIC(10,7),
    max_lon NUMERIC(10,7),
    min_lat NUMERIC(10,7),
    min_lon NUMERIC(10,7),
    centroid_lat NUMERIC(10,7),
    centroid_lon NUMERIC(10,7),
    city_municipality_id INTEGER,
    province_id INTEGER,
    region_id INTEGER,
    PRIMARY KEY (changeset_id),
    FOREIGN KEY (user_id) REFERENCES changesets."users" (user_id),
    FOREIGN KEY (city_municipality_id) REFERENCES changesets."cities_municipalities" (city_municipality_id),
    FOREIGN KEY (province_id) REFERENCES changesets."provinces" (province_id),
    FOREIGN KEY (region_id) REFERENCES changesets."regions" (region_id)
);