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
    'ETL_Server_Access_Log_Processing',
    default_args = default_args,
    description = 'my first DAG',
    schedule_interval = timedelta(days=1),
)

# Task 1.3 : Unzip data
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag = dag 
)

# Task 1.4 : Extract data from csv file
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag = dag,
)

# Task 1.5 : Extract data from tsv file
extract_data_from_tsv= BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f5-7 tollplaza-data.tsv > tsv_data.csv',
    dag = dag,
)

# Task 1.6 : Extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'awk "NF{print $(NF-1),$NF}"  OFS="\t"  payment-data.txt > fixed_width_data.csv',
    dag = dag,
)

# Task 1.7 : Consolidate data extracted from previous tasks
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag = dag,
)

#Task 1.8 : Transform and load the data
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'awk "$5 = toupper($5)" < extracted_data.csv > transformed_data.csv',
    dag = dag,
)

#Task 1.9 : Define the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
