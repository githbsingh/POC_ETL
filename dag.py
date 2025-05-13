import datetime
from airflow.operators.bash_operator import BashOperator 

from airflow import DAG
#from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'depends_on_past': False,
    'email' : ['manoharkumarnda2010@gmail.com'],
    'emails_on_failure' : False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG('employee_data',
            default_args=default_args,
            description='Runs on extrenal Python Script',
            schedule_interval='@daily',
            catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/extract.py'
    )
