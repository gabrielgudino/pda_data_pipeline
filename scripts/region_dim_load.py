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


# Función para extraer regiones y países de los archivos


def get_regions_from_files():
    regions = set()  # Usamos un set para evitar duplicados

    # Recorrer todos los archivos .txt en el directorio
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.txt'):
            file_path = os.path.join(DATA_DIR, filename)
            # Abrir y leer el archivo
            with open(file_path, 'r') as file:
                data = json.load(file)
                country = data['location']['country']
                region = data['location'].get('region')  # Obtener el campo 'region'
                location_name = data['location']['name']  # Obtener el location_name

                # Si la región está vacía o es nula, usar location_name
                if not region:
                    region = location_name
                # Añadir la región y el país si no están vacíos
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


if __name__ == "__main__":
    regions = get_regions_from_files()
    insert_regions_into_region_dim(regions)
    print("Proceso de carga de regiones completado.")

# Cerrar la conexión
cursor.close()
conn.close()
