from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess

# Argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# DAG principal
with DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL para cargar datos en Redshift',
    schedule_interval='*/10 * * * *',  # Corre cada 10 minutos
    catchup=False,
) as dag:

    # Función para ejecutar un script Python
    def run_script(script_name):
        script_path = f'/opt/airflow/scripts/{script_name}'
        subprocess.run(['python', script_path], check=True)

    # Nueva tarea para ejecutar el script ingest.py
    ingest_data = PythonOperator(
        task_id='ingest_data',
        python_callable=run_script,
        op_args=['ingest.py'],
    )

    # Tareas para cargar las dimensiones
    load_country_dim = PythonOperator(
        task_id='load_country_dim',
        python_callable=run_script,
        op_args=['country_dim_load.py'],
    )

    load_region_dim = PythonOperator(
        task_id='load_region_dim',
        python_callable=run_script,
        op_args=['region_dim_load.py'],
    )

    load_location_dim = PythonOperator(
        task_id='load_location_dim',
        python_callable=run_script,
        op_args=['location_dim_load.py'],
    )

    load_condition_dim = PythonOperator(
        task_id='load_condition_dim',
        python_callable=run_script,
        op_args=['condition_dim_load.py'],
    )

    load_date_dim = PythonOperator(
        task_id='load_date_dim',
        python_callable=run_script,
        op_args=['date_dim_load.py'],
    )

    load_weather_fact = PythonOperator(
        task_id='load_weather_fact',
        python_callable=run_script,
        op_args=['weather_fact_load.py'],
    )

    # Definir el orden de las tareas de acuerdo al modelo dimensional
    # Primero se ejecuta ingest_data, luego las demás tareas
    ingest_data >> load_country_dim >> load_region_dim >> load_location_dim \
                >> load_condition_dim >> load_date_dim >> load_weather_fact
