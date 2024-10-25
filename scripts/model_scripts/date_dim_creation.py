import os
import psycopg2
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener credenciales y datos de conexión de Redshift
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")

# Ruta al archivo SQL para la creación de date_dim
SQL_FILE_PATH = "/opt/airflow/sql/date_dim.sql"


# Crear la conexión a Redshift
def create_redshift_connection():
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    # Establecer el esquema por defecto
    cursor = conn.cursor()
    cursor.execute(f'SET search_path TO "{REDSHIFT_SCHEMA}";')
    conn.commit()
    cursor.close()
    return conn


# Ejecutar el script SQL de creación de la tabla date_dim
def create_date_dim_table():
    connection = create_redshift_connection()
    cursor = connection.cursor()
    try:
        with open(SQL_FILE_PATH, 'r') as file:
            sql_script = file.read()
        cursor.execute(sql_script)
        connection.commit()
        print(f"Tabla `date_dim` creada exitosamente usando {SQL_FILE_PATH}")
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    create_date_dim_table()
