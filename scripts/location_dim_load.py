import os
import psycopg2
from dotenv import load_dotenv
import datetime
import json

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener la fecha actual en el formato requerido
CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d").replace("-", "_")

# Directorio donde están los archivos de datos con base en la fecha actual
DATA_DIR = f'./data/{CURRENT_DATE}'

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
conn.commit()  # Hacer commit del cambio de search_path
print(f"Esquema establecido a: {REDSHIFT_SCHEMA}")


# Función para extraer las ubicaciones de los archivos


def get_locations_from_files():
    locations = set()  # Usamos un set para evitar duplicados

    # Recorrer todos los archivos .txt en el directorio
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.txt'):
            file_path = os.path.join(DATA_DIR, filename)
            # Abrir y leer el archivo
            with open(file_path, 'r') as file:
                data = json.load(file)
                location_name = data['location']['name']
                region = data['location'].get('region', location_name)
                # Verificar si el campo region está vacío o es nulo
                if not region:
                    region = location_name
                lat = data['location']['lat']
                lon = data['location']['lon']
                tz_id = data['location']['tz_id']
                # Añadir la ubicación si no está vacía
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
            print(f"""
                  Región no encontrada para la ubicación {location_name}.
                  Inserta la región primero: {region}""")

    # Hacer commit para guardar los cambios
    conn.commit()


if __name__ == "__main__":
    locations = get_locations_from_files()
    insert_locations_into_location_dim(locations)
    print("Proceso de carga de ubicaciones completado.")

# Cerrar la conexión
cursor.close()
conn.close()
