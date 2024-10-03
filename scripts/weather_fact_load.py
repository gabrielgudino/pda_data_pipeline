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

# Función para extraer datos de los archivos y cargar en weather_fact
def load_weather_data():
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.txt'):
            file_path = os.path.join(DATA_DIR, filename)

            # Abrir y leer el archivo
            with open(file_path, 'r') as file:
                data = json.load(file)

                # Extraer los datos necesarios
                location_name = data['location']['name']
                condition_code = data['current']['condition']['code']
                last_updated_epoch = data['current']['last_updated_epoch']
                temp_c = data['current']['temp_c']
                temp_f = data['current']['temp_f']
                is_day = data['current']['is_day']
                wind_mph = data['current']['wind_mph']
                wind_kph = data['current']['wind_kph']
                wind_degree = data['current']['wind_degree']
                wind_dir = data['current']['wind_dir']
                pressure_mb = data['current']['pressure_mb']
                pressure_in = data['current']['pressure_in']
                precip_mm = data['current']['precip_mm']
                precip_in = data['current']['precip_in']
                humidity = data['current']['humidity']
                cloud = data['current']['cloud']
                feelslike_c = data['current']['feelslike_c']
                feelslike_f = data['current']['feelslike_f']
                windchill_c = data['current']['windchill_c']
                windchill_f = data['current']['windchill_f']
                heatindex_c = data['current']['heatindex_c']
                heatindex_f = data['current']['heatindex_f']
                dewpoint_c = data['current']['dewpoint_c']
                dewpoint_f = data['current']['dewpoint_f']
                vis_km = data['current']['vis_km']
                vis_miles = data['current']['vis_miles']
                uv = data['current']['uv']
                gust_mph = data['current']['gust_mph']
                gust_kph = data['current']['gust_kph']

                # Convertir last_updated_epoch a fecha
                last_updated_date = datetime.datetime.utcfromtimestamp(last_updated_epoch).date()

                # Obtener el date_id desde la tabla date_dim
                cursor.execute("SELECT date_id FROM date_dim WHERE date = %s", (last_updated_date,))
                date_result = cursor.fetchone()
                if date_result:
                    date_id = date_result[0]
                else:
                    print(f"No se encontró un date_id para la fecha {last_updated_date}. Saltando registro.")
                    continue

                # Obtener location_id desde location_dim (suponiendo que el nombre de la ubicación es único)
                cursor.execute("SELECT location_id FROM location_dim WHERE location_name = %s", (location_name,))
                location_result = cursor.fetchone()
                if location_result:
                    location_id = location_result[0]
                else:
                    print(f"No se encontró un location_id para la ubicación {location_name}. Saltando registro.")
                    continue

                # Obtener condition_id desde condition_dim
                cursor.execute("SELECT condition_id FROM condition_dim WHERE condition_code = %s", (condition_code,))
                condition_result = cursor.fetchone()
                if condition_result:
                    condition_id = condition_result[0]
                else:
                    print(f"No se encontró un condition_id para el código {condition_code}. Saltando registro.")
                    continue

                # Insertar en weather_fact
                cursor.execute("""
                    INSERT INTO weather_fact (location_id, condition_id, date_id, temp_c, temp_f, is_day, wind_mph, wind_kph,
                                              wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, 
                                              cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, 
                                              dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    location_id, condition_id, date_id, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, 
                    pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, 
                    windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph
                ))

    conn.commit()

if __name__ == "__main__":
    load_weather_data()
    print("Datos meteorológicos cargados en weather_fact exitosamente.")
    
# Cerrar la conexión
cursor.close()
conn.close()
