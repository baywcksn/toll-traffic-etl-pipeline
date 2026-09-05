from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Bayu',
    'start_date': datetime.today(),
    'email': ['bayuwcksn01@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule='@daily'
)


unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf /opt/airflow/data/raw/tolldata.tgz -C /opt/airflow/data/raw',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d, -f1-4 /opt/airflow/data/raw/vehicle-data.csv > /opt/airflow/data/processed/csv_data.csv',
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -f5-7 /opt/airflow/data/raw/tollplaza-data.tsv | tr '\\t' ',' | tr -d '\\r' > /opt/airflow/data/processed/tsv_data.csv",
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="awk '{print substr($0,59,3) \",\" substr($0,63,5)}' /opt/airflow/data/raw/payment-data.txt > /opt/airflow/data/processed/fixed_width_data.csv",
    dag=dag
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d, /opt/airflow/data/processed/csv_data.csv /opt/airflow/data/processed/tsv_data.csv /opt/airflow/data/processed/fixed_width_data.csv > /opt/airflow/data/processed/extracted_data.csv',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command="tr '[:lower:]' '[:upper:]' < /opt/airflow/data/processed/extracted_data.csv > /opt/airflow/data/processed/transformed_data.csv",
    dag=dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data