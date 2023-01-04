# Scenario
# You are a data engineer at a data analytics consulting company. 
# You have been assigned to a project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas. 
# Each highway is operated by a different toll operator with a different IT setup that uses different file formats. 
# Your job is to collect data available in different formats and consolidate it into a single file.

# Objectives
# In this assignment you will author an Apache Airflow DAG that will:

# Extract data from a csv file
# Extract data from a tsv file
# Extract data from a fixed width file
# Transform the data
# Load the transformed data into the staging area

import airflow
from airflow import DAG
from datetime import timedelta
from airflow.bash_operators import BashOperator
from airflow.python_operators import PythonOperator
from airflow.utils.dates import day_ago


defaults_args = {
    "owner": "Adedoyin Samuel",
    "start_date": day_ago(0),
    "email": ["adedoyinsamuel25@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":True,
    "retries":1,
    "retry_delay": timedelta(minutes=5)
}


dag = DAG(
    dag_id = "ETL_toll_data",
    schedule = "@daily",
    defaults_args=defaults_args,
    description="Apache Airflow Final Assignment"
)


download = BashOperator(
    task_id = "download data"
    bash_command = 'curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz \
        --output home/project/airflow/dags/finalassignment/stagging/tolldata.tgz',
    dag = dag
)

unzip = BashOperator(
    task_id = "unzip"
    bash_command = 'tar zxvf home/project/airflow/dags/finalassignment/stagging/tolldata.tgz',
    dag = dag
)

extract_data_from_csv= BashOperator( 
    task_id = "extract_data_from_csv"
    bash_command = 'cut -d"," -f1-4 home/project/airflow/dags/finalassignment/vehicle-data.csv\
        > home/project/airflow/dags/finalassignment/stagging/csv_data.csv,'
    dag = dag
)

extract_data_from_tsv= BashOperator( 
    task_id = "extract_data_from_tsv"
    bash_command = 'cut -d"," -f5-7, home/project/airflow/dags/finalassignment/tollplaza-data.tsv\
        > home/project/airflow/dags/finalassignment/stagging/tsv_data.csv,'
    dag = dag
)

extract_data_from_fixed_width= BashOperator( 
    task_id = "extract_data_from_fixed_width"
    bash_command = 'cut -d"," -f6-7 home/project/airflow/dags/finalassignment/payment-data.txt\
        > home/project/airflow/dags/finalassignment/stagging/fixed_width_data.csv,'
    dag = dag
)

consolidate_data= BashOperator( 
    task_id = "consolidate_data"
    bash_command = 'paste home/project/airflow/dags/finalassignment/stagging/csv_data.csv\
        home/project/airflow/dags/finalassignment/stagging/tsv_data.csv\
        home/project/airflow/dags/finalassignment/stagging/fixed_width_data.csv \
        > home/project/airflow/dags/finalassignment/stagging/extracted_data.csv '
    dag = dag
)

transform = BashOperator( 
    task_id = "consolidate_data"
    bash_command = 'tr "[a-z]" "[A-Z]" -f7 home/project/airflow/dags/finalassignment/stagging/extracted_data.csv\
        > home/project/airflow/dags/finalassignment/stagging/transformed_data.csv'
    dag = dag
)

download > unzip > extract_data_from_csv > extract_data_from_tsv > extract_data_from_fixed_width > consolidate_data > transform