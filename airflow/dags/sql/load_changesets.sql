-- Users dim table
INSERT INTO changesets.users(
    user_id,
    username
    )
    SELECT
        uid,
        username
    FROM staging.stg_osm_changesets
    ON CONFLICT DO NOTHING
;

-- Changesets fact table
WITH r as(
    SELECT region_id, region_relation_id
    FROM changesets.regions
    WHERE end_date = '9999-12-31'::date
    ), p as(
    SELECT province_id, province_relation_id
    FROM changesets.provinces
    WHERE end_date = '9999-12-31'::date
    ), cm as(
    SELECT city_municipality_id, city_municipality_relation_id
    FROM changesets.cities_municipalities
    WHERE end_date = '9999-12-31'::date
    )
    INSERT INTO changesets.osm_changesets as oc(
        changeset_id,
        user_id,
        created_at,
        closed_at,
        num_changes,
        max_lat,
        max_lon,
        min_lat,
        min_lon,
        centroid_lat,
        centroid_lon,
        city_municipality_id,
        province_id,
        region_id
        )
        SELECT
            id,
            uid,
            created_at,
            closed_at,
            num_changes,
            max_lat,
            max_lon,
            min_lat,
            min_lon,
            centroid_lat,
            centroid_lon,
            city_municipality_id,
            province_id,
            region_id
        FROM staging.stg_osm_changesets as sc
        LEFT JOIN r
            ON sc.region_relation_id = r.region_relation_id
        LEFT JOIN p
            ON sc.province_relation_id = p.province_relation_id
        LEFT JOIN cm
            ON sc.city_municipality_relation_id = cm.city_municipality_relation_id
    ON CONFLICT DO NOTHING
;

-- Truncate staging table
TRUNCATE staging.stg_osm_changesets;