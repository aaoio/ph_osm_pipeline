from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import json
import numpy as np
import os
import pandas as pd
import re
from scripts.paths import *
from sqlalchemy.sql import text

class GeogTables:

    def __init__(self, wikidata_paths_list, overpass_paths_list):
        self.wd_paths = wikidata_paths_list
        self.op_paths = overpass_paths_list
        self.isin_columns = None

    def create_df(self, json_path):
        """
        Create a dataframe for a JSON file
        """
        wd_columns = [
            'itemLabel.value',
            'item.value',
            'population.value',
            'withinLabel.value',
            'within.value'
        ]
        wd_cm_columns = [
            'itemLabel.value',
            'item.value',
            'instanceofLabel.value',
            'incomeclassLabel.value',
            'population.value',
            'withinLabel.value',
            'within.value',
            'area.value'
        ]
        op_columns = [
            'properties.@id',
            'properties.name',
            'properties.wikidata'
        ]

        isin_pattern = re.compile(r'properties.is_in:?\w*')

        with open(json_path) as f:
            json_file = json.loads(f.read())
        
        if json_path in self.wd_paths:
            data = json_file['results']['bindings']
            df = pd.json_normalize(data)
            if json_path == self.wd_paths[2]:
                df = df[wd_cm_columns]
                city_type_mask = df['instanceofLabel.value'].isin([
                    'municipality of the Philippines',
                    'component city',
                    'highly urbanized city',
                    'independent component city'
                ])
                df = df[city_type_mask]
            else:
                df = df[wd_columns]
            for x in ['item.value', 'within.value']:
                df[x] = df[x].map(lambda y: y[32:])
        
        elif json_path in self.op_paths:
            data = json_file['features']
            df = pd.json_normalize(data)
            isin_columns = []
            for col in df.columns:
                if re.match(isin_pattern, col):
                    isin_columns.append(col)
            self.isin_columns = isin_columns
            if json_path == self.op_paths[2]:
                df = df[op_columns + isin_columns]
            else:
                df = df[op_columns]
            relation_mask = df['properties.@id']\
                .str.startswith('relation/')
            df = df[relation_mask]
            df['properties.@id'] = df['properties.@id'].map(
                lambda x: x[9:] if pd.isna(x) == False else x
            )
            df['properties.wikidata'] = df['properties.wikidata'].map(
                lambda x: x[1:] if pd.isna(x) == False else x
            )

        return df

    def regions_table(self, wikidata_r_df, overpass_r_df):
        """
        Prepare data for populating changesets.regions table
        """
        r_df = pd.merge(
            overpass_r_df,
            wikidata_r_df,
            left_on='properties.wikidata',
            right_on='item.value'
        )
        r_df = r_df[[
            'properties.@id',
            'properties.name',
            'properties.wikidata',
            'population.value'
        ]]
        r_df.columns = [
            'region_relation_id',
            'region_name',
            'region_wikidata_item',
            'region_population'
        ]
        r_df.name = 'regions'
        return r_df

    def provinces_table(self, wikidata_p_df, overpass_p_df):
        """
        Prepare data for populating changesets.provinces table
        """
        p_df = pd.merge(
            overpass_p_df,
            wikidata_p_df,
            left_on='properties.wikidata',
            right_on='item.value'
        )
        p_df = p_df[[
            'properties.@id',
            'properties.name',
            'properties.wikidata',
            'population.value',
            'withinLabel.value',
            'within.value'
        ]]
        p_df.columns = [
            'province_relation_id',
            'province_name',
            'province_wikidata_item',
            'province_population',
            'province_is_within',
            'province_is_within_wikidata_item'
        ]
        p_df.name = 'provinces'
        return p_df


    def cities_municipalities_table(
        self,
        wikidata_cm_df,
        overpass_cm_df
    ):
        """
        Prepare data for populating changesets.cities_municipalities
        table in database
        """
        # This process begins by joining the Overpass GeoJSON to the
        # Wikidata dataframe on their wikidata entity number.
        # For cities in the Overpass GeoJSON that do not have 'wikidata'
        # under 'properties', this function checks for an object that
        # indicates what larger administrative territory the city falls
        # under and joins on that 'is_in' column.

        arranged_df_columns = [
            'properties.@id',
            'properties.name',
            'item.value',
            'instanceofLabel.value',
            'incomeclassLabel.value',
            'population.value',
            'withinLabel.value',
            'within.value',
            'area.value'
        ]

        table_columns = [
            'city_municipality_relation_id',
            'city_municipality_name',
            'city_municipality_wikidata_item',
            'city_municipality_type',
            'city_municipality_income_class',               
            'city_municipality_population',
            'city_municipality_is_within',
            'city_municipality_is_within_wikidata_item',
            'city_municipality_area',
        ]

        cm_df = pd.DataFrame([], columns=arranged_df_columns)
        
        # With wikidata property
        with_wd_mask = overpass_cm_df['properties.wikidata'].notnull()
        op_with_wikidata = overpass_cm_df[with_wd_mask]

        merge_on_wd = pd.merge(
            op_with_wikidata,
            wikidata_cm_df,
            left_on='properties.wikidata',
            right_on='item.value'
        )
        
        merge_on_wd = merge_on_wd[arranged_df_columns]
        cm_df = pd.concat([cm_df, merge_on_wd], ignore_index=True)

        # Without wikidata property, with is_in property
        op_without_wikidata = overpass_cm_df[~with_wd_mask]
        op_without_wikidata_with_isin = op_without_wikidata.dropna(
            how='all',
            subset=self.isin_columns
        )
        merge_on_isin_dfs = []
        for isin_col in self.isin_columns:
            merge_on_isin = pd.merge(
                op_without_wikidata_with_isin,
                wikidata_cm_df,
                left_on=['properties.name', isin_col],
                right_on=['itemLabel.value', 'withinLabel.value']
            )
            merge_on_isin_dfs.append(merge_on_isin)
        
        merge_on_isin = pd.concat(merge_on_isin_dfs)
        merge_on_isin = merge_on_isin[arranged_df_columns]

        cm_df = pd.concat([cm_df, merge_on_isin], ignore_index=True)
        cm_df.columns = table_columns
        cm_df.name = 'cities_municipalities'
        return cm_df

def geog_dims(wikidata_paths_list, overpass_paths_list,
    postgres_conn_id):

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = hook.get_sqlalchemy_engine()

    gt = GeogTables(wikidata_paths_list, overpass_paths_list)

    wikidata_r_df = gt.create_df(wikidata_paths_list[0])
    overpass_r_df = gt.create_df(overpass_paths_list[0])
    regions_table = gt.regions_table(wikidata_r_df, overpass_r_df)

    wikidata_p_df = gt.create_df(wikidata_paths_list[1])
    overpass_p_df = gt.create_df(overpass_paths_list[1])
    provinces_table = gt.provinces_table(wikidata_p_df, overpass_p_df)
    
    wikidata_cm_df = gt.create_df(wikidata_paths_list[2])
    overpass_cm_df = gt.create_df(overpass_paths_list[2])
    cities_municipalities_table = gt.cities_municipalities_table(
        wikidata_cm_df,
        overpass_cm_df
    )

    # Staging
    
    dfs = [regions_table, provinces_table, cities_municipalities_table]

    for df in dfs:
        engine.execute(text(f'TRUNCATE TABLE staging.stg_{df.name}').\
            execution_options(autocommit=True))
        df.to_sql(
            f'stg_{df.name}',
            engine,
            schema='staging',
            if_exists='append',
            index=False
        )
