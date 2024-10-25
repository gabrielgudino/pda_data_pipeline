import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
SQL_FILE = "/opt/airflow/sql/country_dim.sql"
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")


def create_country_dim():
    conn = psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT"),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )
    cursor = conn.cursor()
    cursor.execute(f'SET search_path TO "{REDSHIFT_SCHEMA}";')
    conn.commit()

    with open(SQL_FILE, 'r') as file:
        sql_script = file.read()
        cursor.execute(sql_script)
        conn.commit()
        print("Tabla country_dim creada exitosamente.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    create_country_dim()
