import os
import psycopg2
from dotenv import load_dotenv
import datetime

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener la fecha actual en el formato requerido
CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d").replace("-", "_")

# Obtener credenciales y datos de conexión de Redshift
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")

# Conectar a Redshift usando psycopg2
conn = psycopg2.connect(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    dbname=REDSHIFT_DB,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD
)
# Crear un cursor para ejecutar consultas
cursor = conn.cursor()

# Establecer el esquema
cursor.execute(f'SET search_path TO "{REDSHIFT_SCHEMA}";')
conn.commit()
print(f"Esquema establecido a: {REDSHIFT_SCHEMA}")

# Función para obtener los países únicos desde la tabla de staging
def get_countries_from_staging():
    countries = set()  # Usamos un set para evitar duplicados

    # Consulta para obtener los países de los registros insertados en los últimos 30 minutos
    query = """
    SELECT DISTINCT country
    FROM weather_staging
    WHERE created_at >= dateadd(minute, -30, GETDATE());
    """
    
    cursor.execute(query)
    results = cursor.fetchall()

    # Añadir los países a la lista
    for row in results:
        country = row[0]
        if country:
            countries.add(country)

    return countries

# Función para insertar países en la tabla country_dim
def insert_countries_into_country_dim(countries):
    for country in countries:
        # Verificar si el país ya existe en la tabla
        cursor.execute(
            "SELECT country_id FROM country_dim WHERE country_name = %s", (country,)
        )
        result = cursor.fetchone()

        # Si no existe, insertar el país
        if result is None:
            cursor.execute(
                "INSERT INTO country_dim (country_name) VALUES (%s)", (country,)
            )
            print(f"País insertado: {country}")
        else:
            print(f"País ya existente: {country}")

    # Hacer commit para guardar los cambios
    conn.commit()

# Proceso principal
if __name__ == "__main__":
    countries = get_countries_from_staging()
    if countries:
        insert_countries_into_country_dim(countries)
        print("Proceso de carga de países completado.")
    else:
        print("No se encontraron países nuevos en los últimos 30 minutos.")

# Cerrar la conexión
cursor.close()
conn.close()
