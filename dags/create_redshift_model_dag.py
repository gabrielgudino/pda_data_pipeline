from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess


# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    'create_redshift_model',
    default_args=default_args,
    description='DAG para crear el modelo en Redshift',
    schedule_interval=None,  # Sin programación, solo se ejecuta manualmente
    catchup=False
)


# Definir funciones para cada tarea de ejecución de SQL
def run_script(script_path):
    subprocess.run(['python3', script_path], check=True)


# Definir las tareas en el DAG según el orden especificado
create_country_dim = PythonOperator(
    task_id='create_country_dim',
    python_callable=run_script,
    op_args=['/opt/airflow/scripts/model_scripts/country_dim_creation.py'],
    dag=dag
)

create_region_dim = PythonOperator(
    task_id='create_region_dim',
    python_callable=run_script,
    op_args=['/opt/airflow/scripts/model_scripts/region_dim_creation.py'],
    dag=dag
)

create_location_dim = PythonOperator(
    task_id='create_location_dim',
    python_callable=run_script,
    op_args=['/opt/airflow/scripts/model_scripts/location_dim_creation.py'],
    dag=dag
)

create_condition_dim = PythonOperator(
    task_id='create_condition_dim',
    python_callable=run_script,
    op_args=['/opt/airflow/scripts/model_scripts/condition_dim_creation.py'],
    dag=dag
)

create_date_dim = PythonOperator(
    task_id='create_date_dim',
    python_callable=run_script,
    op_args=['/opt/airflow/scripts/model_scripts/date_dim_creation.py'],
    dag=dag
)

create_weather_fact = PythonOperator(
    task_id='create_weather_fact',
    python_callable=run_script,
    op_args=['/opt/airflow/scripts/model_scripts/weather_fact_creation.py'],
    dag=dag
)

create_weather_staging = PythonOperator(
    task_id='create_weather_staging',
    python_callable=run_script,
    op_args=['/opt/airflow/scripts/model_scripts/weather_staging_creation.py'],
    dag=dag
)

load_date_dim = PythonOperator(
    task_id='load_date_dim',
    python_callable=run_script,
    op_args=['/opt/airflow/scripts/model_scripts/date_dim_load.py'],
    dag=dag
)

# Definir el orden de ejecución
create_country_dim >> create_region_dim >> create_location_dim >> create_condition_dim
create_condition_dim >> create_date_dim >> create_weather_fact >> create_weather_staging
create_weather_staging >> load_date_dim
