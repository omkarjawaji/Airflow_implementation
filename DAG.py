from datetime import timedelta

from airflow import DAG 

from airflow.operators.bash_operator import BashOperator

from airflow.utils.dates import days_ago


default_args = {
    "owner": "Omkar Jawaji",
    'start_date': days_ago(0),
    'email': ['omkar@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my-first-DAG',
    default_args = default_args,
    description = 'my first DAG',
    schedul_interval = timedelta(days=1),
)

# Extract Pipeline
extract = Bashoperator(
    task_id = 'extract',
    bash_command = ' cut -d":" -f1,3,6 /etc/passwd > /home/project/airflow/dags/extracted-data.txt',
    dag = dag,
)

# Transform and Load Pipeline
transform_and_load = BashOperator(
    task_id = 'trans_laod',
    bash_command = ' tr ":" "," < < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.csv '
    dag = dag,
)

# Task Pipeline

extract >> transform_and_load
