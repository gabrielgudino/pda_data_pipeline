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


# Función para obtener las regiones y países únicos desde la tabla de staging
def get_regions_from_staging():
    regions = set()  # Usamos un set para evitar duplicados

    # Consulta para obtener las regiones y países de los registros insertados
    query = """
    SELECT DISTINCT location_name,
    COALESCE(NULLIF(region, ''), location_name) AS region,
    country
    FROM weather_staging
    WHERE created_at >= dateadd(minute, -30, GETDATE());
    """

    cursor.execute(query)
    results = cursor.fetchall()

    # Añadir las regiones y países al set
    for row in results:
        region = row[1]
        country = row[2]

        if region and country:
            regions.add((region, country))

    return regions


# Función para insertar regiones en la tabla region_dim
def insert_regions_into_region_dim(regions):
    for region, country in regions:
        # Verificar si el país ya existe en la tabla country_dim
        cursor.execute("""
                       SELECT country_id FROM country_dim WHERE country_name = %s
                       """, (country,))
        country_result = cursor.fetchone()

        if country_result is not None:
            country_id = country_result[0]  # Obtener el ID del país

            # Verificar si la región ya existe en la tabla region_dim
            cursor.execute("""
                           SELECT region_id FROM region_dim
                           WHERE region_name = %s AND country_id = %s
                           """, (region, country_id))
            region_result = cursor.fetchone()

            # Si la región no existe, insertarla
            if region_result is None:
                cursor.execute("""
                               INSERT INTO region_dim (region_name, country_id)
                               VALUES (%s, %s)
                               """, (region, country_id))
                print(f"Región insertada: {region} (País: {country})")
            else:
                print(f"Región ya existente: {region} (País: {country})")
        else:
            print(f"""País no encontrado para la región {region}.
                  Inserta el país primero: {country}""")

    # Hacer commit para guardar los cambios
    conn.commit()


# Proceso principal
if __name__ == "__main__":
    regions = get_regions_from_staging()
    if regions:
        insert_regions_into_region_dim(regions)
        print("Proceso de carga de regiones completado.")
    else:
        print("No se encontraron regiones nuevas en los últimos 30 minutos.")

# Cerrar la conexión
cursor.close()
conn.close()
