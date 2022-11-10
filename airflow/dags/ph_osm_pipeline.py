from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres \
    import PostgresOperator
from airflow.contrib.operators.spark_submit_operator \
    import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import scripts.geog_tables as gt
import scripts.overpass as op
import scripts.sequence as sq
import scripts.wikidata as wd

with DAG(
    'PH_OSMPipelineDag',
    schedule_interval='@weekly',
    start_date=datetime(2022,10,24),
    catchup=False
) as dag:

    mkdir_task = BashOperator(
        task_id='make_directories',
        bash_command="""
mkdir -p /opt/airflow/data/overpass/`date -d$LOGICAL_DATE +%Y_%m_%d` \
/opt/airflow/data/wikidata/`date -d$LOGICAL_DATE +%Y_%m_%d`
""",
        env={'LOGICAL_DATE': '{{ ds }}'}
    )

    build_db_task = PostgresOperator(
        task_id='build_db',
        postgres_conn_id='postgres_localhost',
        sql='sql/db_build.sql',
    )

    overpass_task = PythonOperator(
        task_id = 'extract_overpass',
        python_callable=op.extract_overpass,
        provide_context=True
    )

    wikidata_task = PythonOperator(
        task_id = 'extract_wikidata',
        python_callable=wd.extract_wikidata,
        provide_context=True
    )

    sequence_range_task = PythonOperator(
        task_id='get_sequence_range',
        python_callable=sq.get_sequence_range,
        op_args=['postgres_localhost'],
        do_xcom_push=True
    )

    download_repls_task = SparkSubmitOperator(
        task_id='dl_repls',
        application='/usr/local/spark/app/dl_replication_files.py',
        name='spark_app',
        conn_id='spark_local',
        verbose=1,
        application_args=[sequence_range_task.output]
    )

    stage_geog_task = PythonOperator(
        task_id='stage_geog_dims',
        python_callable=gt.geog_dims,
        op_args = [
            wikidata_task.output,
            overpass_task.output,
            'postgres_localhost'
        ]
    )

    load_geog_task = PostgresOperator(
        task_id='load_geog_dims',
        postgres_conn_id='postgres_localhost',
        sql='sql/load.sql',
    )

    parse_repls_task = SparkSubmitOperator(
        task_id='parse_repls',
        application='/usr/local/spark/app/parse_replication_files.py',
        name='spark_app',
        conn_id='spark_local',
        jars='/usr/local/spark/resources/postgresql-42.5.0.jar',
        packages='com.databricks:spark-xml_2.12:0.15.0',
        application_args=overpass_task.output,
        verbose=1
    )

    

build_db_task >> sequence_range_task >> download_repls_task >> parse_repls_task
build_db_task >> stage_geog_task
mkdir_task >> [overpass_task, wikidata_task] >> stage_geog_task >> load_geog_task
overpass_task >> parse_repls_task