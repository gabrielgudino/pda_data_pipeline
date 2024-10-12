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

cursor.execute(f'SET search_path TO "{REDSHIFT_SCHEMA}";')
conn.commit()
print(f"Esquema establecido a: {REDSHIFT_SCHEMA}")


# Función para obtener las ubicaciones únicas desde la tabla de staging


def get_locations_from_staging():
    locations = set()  # Usamos un set para evitar duplicados

    # Consulta para obtener las ubicaciones insertadas en los últimos 30 minutos
    query = """
    SELECT DISTINCT location_name,
    COALESCE(NULLIF(region, ''), location_name) AS region,
    lat, lon, tz_id
    FROM weather_staging
    WHERE created_at >= dateadd(minute, -30, GETDATE());
    """

    cursor.execute(query)
    results = cursor.fetchall()

    # Añadir las ubicaciones al set
    for row in results:
        location_name = row[0]
        region = row[1] or location_name
        lat = row[2]
        lon = row[3]
        tz_id = row[4]

        if location_name:
            locations.add((location_name, region, lat, lon, tz_id))

    return locations


# Función para insertar ubicaciones en la tabla location_dim


def insert_locations_into_location_dim(locations):
    for location_name, region, lat, lon, tz_id in locations:
        # Verificar si la región ya existe en la tabla region_dim
        cursor.execute("""
                       SELECT region_id FROM region_dim WHERE region_name = %s
                       """, (region,))
        region_result = cursor.fetchone()
        if region_result is not None:
            region_id = region_result[0]  # Obtener el ID de la región
            # Verificar si la ubicación ya existe en la tabla location_dim
            cursor.execute("""
                           SELECT location_id FROM location_dim
                           WHERE location_name = %s AND region_id = %s
                           """, (location_name, region_id))
            location_result = cursor.fetchone()
            # Si la ubicación no existe, insertarla
            if location_result is None:
                cursor.execute("""
                    INSERT INTO location_dim (location_name, region_id, lat, lon, tz_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, (location_name, region_id, lat, lon, tz_id))
                print(f"Ubicación insertada: {location_name} (Región: {region})")
            else:
                print(f"Ubicación ya existente: {location_name} (Región: {region})")
        else:
            print(f"""Región no encontrada para la ubicación {location_name}.
                  Inserta la región primero: {region}""")

    # Hacer commit para guardar los cambios
    conn.commit()


# Proceso principal
if __name__ == "__main__":
    locations = get_locations_from_staging()
    if locations:
        insert_locations_into_location_dim(locations)
        print("Proceso de carga de ubicaciones completado.")
    else:
        print("No se encontraron ubicaciones nuevas en los últimos 30 minutos.")

# Cerrar la conexión
cursor.close()
conn.close()
