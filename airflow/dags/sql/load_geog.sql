-- Load regions

WITH n AS (
	SELECT *
	FROM staging.stg_regions AS sr
	WHERE NOT EXISTS(
		SELECT *
		FROM changesets.regions AS r
		WHERE r.region_relation_id  = sr.region_relation_id AND
			  r.region_name = sr.region_name AND
			  r.region_wikidata_item = sr.region_wikidata_item AND
			  r.region_population = sr.region_population 
		)
	), u AS (
	UPDATE changesets.regions AS r
    SET end_date = CURRENT_DATE
	WHERE NOT EXISTS(
		SELECT *
		FROM staging.stg_regions AS sr
		WHERE r.region_relation_id  = sr.region_relation_id AND
			  r.region_name = sr.region_name AND
			  r.region_wikidata_item = sr.region_wikidata_item AND
			  r.region_population = sr.region_population 
		) AND end_date = '9999-12-31'::date
	
    )
INSERT INTO changesets.regions(
    region_relation_id,
    region_name,
    region_population,
    region_wikidata_item,
    start_date,
    end_date
    )
    SELECT
        region_relation_id,
        region_name,
        region_population,
        region_wikidata_item,
        CURRENT_DATE,
        '9999-12-31'::date
    FROM n;


-- Load provinces

WITH n AS (
	SELECT *
	FROM staging.stg_provinces AS sp
	WHERE NOT EXISTS(
		SELECT *
		FROM changesets.provinces AS p
		WHERE p.province_relation_id  = sp.province_relation_id AND
			  p.province_name = sp.province_name AND
			  p.province_wikidata_item = sp.province_wikidata_item AND
			  p.province_population = sp.province_population AND
              p.province_is_within = sp.province_is_within AND
              p.province_is_within_wikidata_item = sp.province_is_within_wikidata_item
		)
	), u AS (
	UPDATE changesets.provinces AS p
    SET end_date = CURRENT_DATE
	WHERE NOT EXISTS(
		SELECT *
		FROM staging.stg_provinces AS sp
		WHERE p.province_relation_id  = sp.province_relation_id AND
			  p.province_name = sp.province_name AND
			  p.province_wikidata_item = sp.province_wikidata_item AND
			  p.province_population = sp.province_population AND
              p.province_is_within = sp.province_is_within AND
              p.province_is_within_wikidata_item = sp.province_is_within_wikidata_item
		) AND end_date = '9999-12-31'::date
	
    )
INSERT INTO changesets.provinces(
    province_relation_id,
    province_name,
    province_population,
    province_wikidata_item,
    province_is_within,
    province_is_within_wikidata_item,
    start_date,
    end_date
    )
    SELECT
        province_relation_id,
        province_name,
        province_population,
        province_wikidata_item,
        province_is_within,
        province_is_within_wikidata_item,
        CURRENT_DATE,
        '9999-12-31'::date
    FROM n;

-- Load cities & municipalities

WITH n AS (
	SELECT *
	FROM staging.stg_cities_municipalities AS scm
	WHERE NOT EXISTS(
		SELECT *
		FROM changesets.cities_municipalities AS cm
		WHERE cm.city_municipality_relation_id  = scm.city_municipality_relation_id AND
			  cm.city_municipality_name = scm.city_municipality_name AND
			  cm.city_municipality_wikidata_item = scm.city_municipality_wikidata_item AND
              cm.city_municipality_type = scm.city_municipality_type AND
              cm.city_municipality_income_class = scm.city_municipality_income_class AND
			  cm.city_municipality_population = scm.city_municipality_population AND
              cm.city_municipality_is_within = scm.city_municipality_is_within AND
              cm.city_municipality_is_within_wikidata_item = scm.city_municipality_is_within_wikidata_item AND
              cm.city_municipality_area = scm.city_municipality_area
		)
	), u AS (
	UPDATE changesets.cities_municipalities AS cm
    SET end_date = CURRENT_DATE
	WHERE NOT EXISTS(
		SELECT *
		FROM staging.stg_cities_municipalities AS scm
		WHERE cm.city_municipality_relation_id  = scm.city_municipality_relation_id AND
			  cm.city_municipality_name = scm.city_municipality_name AND
			  cm.city_municipality_wikidata_item = scm.city_municipality_wikidata_item AND
              cm.city_municipality_type = scm.city_municipality_type AND
              cm.city_municipality_income_class = scm.city_municipality_income_class AND
			  cm.city_municipality_population = scm.city_municipality_population AND
              cm.city_municipality_is_within = scm.city_municipality_is_within AND
              cm.city_municipality_is_within_wikidata_item = scm.city_municipality_is_within_wikidata_item AND
              cm.city_municipality_area = scm.city_municipality_area
		) AND end_date = '9999-12-31'::date
	
    )
INSERT INTO changesets.cities_municipalities(
    city_municipality_relation_id,
    city_municipality_name,
    city_municipality_wikidata_item,
    city_municipality_type,
    city_municipality_income_class,
    city_municipality_population,
    city_municipality_is_within,
    city_municipality_is_within_wikidata_item,
    city_municipality_area,
    start_date,
    end_date
    )
    SELECT
        city_municipality_relation_id,
        city_municipality_name,
        city_municipality_wikidata_item,
        city_municipality_type,
        city_municipality_income_class,
        city_municipality_population,
        city_municipality_is_within,
        city_municipality_is_within_wikidata_item,
        city_municipality_area,
        CURRENT_DATE,
        '9999-12-31'::date
    FROM n;

-- Truncate staging tables
TRUNCATE staging.stg_regions;
TRUNCATE staging.stg_provinces;
TRUNCATE staging.stg_cities_municipalities;
