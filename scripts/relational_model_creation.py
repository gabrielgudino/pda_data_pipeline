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
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")  # El esquema deseado, 'public' como valor por defecto

# Ruta a la carpeta que contiene los scripts SQL
SQL_DIR = "./sql"

# Lista con el orden correcto de los archivos SQL
sql_files = [
    "country_dim.sql",
    "region_dim.sql",
    "location_dim.sql",
    "condition_dim.sql",
    "time_dim.sql",
    "date_dim.sql",
    "weather_fact.sql"
]

# Crear la conexión a Redshift usando psycopg2
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
    conn.commit()  # Hacer commit del cambio de search_path
    cursor.close()  # Cerrar el cursor
    print(f"Esquema establecido a: {REDSHIFT_SCHEMA}")
    return conn

# Función para ejecutar un archivo SQL
def execute_sql_file(connection, file_path):
    cursor = connection.cursor()
    with open(file_path, 'r') as file:
        sql_script = file.read()
        cursor.execute(sql_script)
        connection.commit()  # Asegurarse de hacer commit después de cada ejecución
        print(f"Ejecutado: {file_path}")
    cursor.close()  # Cerrar el cursor después de ejecutar

# Ejecutar los scripts SQL en el orden adecuado
def run_sql_scripts():
    connection = create_redshift_connection()
    for sql_file in sql_files:
        file_path = os.path.join(SQL_DIR, sql_file)
        execute_sql_file(connection, file_path)
    connection.close()  # Cerrar la conexión cuando se hayan ejecutado todos los scripts

if __name__ == "__main__":
    run_sql_scripts()
