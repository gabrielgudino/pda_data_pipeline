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
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")  # El esquema deseado, 'public' como valor por defecto

# Conectar a Redshift usando psycopg2
conn = psycopg2.connect(
    host=os.getenv("REDSHIFT_HOST"),
    port=os.getenv("REDSHIFT_PORT"),
    dbname=os.getenv("REDSHIFT_DB"),
    user=os.getenv("REDSHIFT_USER"),
    password=os.getenv("REDSHIFT_PASSWORD")
)
# Crear un cursor para ejecutar consultas
cursor = conn.cursor()

cursor.execute(f'SET search_path TO "{REDSHIFT_SCHEMA}";')
conn.commit()  # Hacer commit del cambio de search_path
print(f"Esquema establecido a: {REDSHIFT_SCHEMA}")

# Función para extraer los países únicos de los archivos
def get_countries_from_files():
    countries = set()  # Usamos un set para evitar duplicados

    # Recorrer todos los archivos .txt en el directorio
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.txt'):
            file_path = os.path.join(DATA_DIR, filename)
            
            # Abrir y leer el archivo
            with open(file_path, 'r') as file:
                data = json.load(file)
                country = data['location']['country']
                
                # Añadir el país si no está vacío
                if country:
                    countries.add(country)

    return countries

# Función para insertar países en la tabla country_dim
def insert_countries_into_country_dim(countries):
    for country in countries:
        # Verificar si el país ya existe en la tabla
        cursor.execute("SELECT country_id FROM country_dim WHERE country_name = %s", (country,))
        result = cursor.fetchone()

        # Si no existe, insertar el país
        if result is None:
            cursor.execute("INSERT INTO country_dim (country_name) VALUES (%s)", (country,))
            print(f"País insertado: {country}")
        else:
            print(f"País ya existente: {country}")

    # Hacer commit para guardar los cambios
    conn.commit()

if __name__ == "__main__":
    countries = get_countries_from_files()
    insert_countries_into_country_dim(countries)
    print("Proceso de carga de países completado.")
    
# Cerrar la conexión
cursor.close()
conn.close()