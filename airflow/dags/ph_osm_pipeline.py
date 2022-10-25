from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import scripts.overpass as op
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

mkdir_task >> [overpass_task, wikidata_task]