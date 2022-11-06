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
        task_id='build_db_task',
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
        task_id='sequence_range_task',
        python_callable=sq.get_sequence_range,
        op_args=['postgres_localhost'],
        do_xcom_push=True
    )

    download_repls = SparkSubmitOperator(
        task_id='dl_repls',
        application='/usr/local/spark/app/dl_replication_files.py',
        name='spark_app',
        conn_id='spark_local',
        verbose=1,
        application_args=[sequence_range_task.output]
    )

    geog_task = PythonOperator(
        task_id='geog_dims',
        python_callable=gt.geog_dims,
        op_args = [
            wikidata_task.output,
            overpass_task.output,
            'postgres_localhost'
        ]
    )
    

build_db_task >> sequence_range_task >> download_repls
build_db_task >> geog_task
mkdir_task >> [overpass_task, wikidata_task] >> geog_task