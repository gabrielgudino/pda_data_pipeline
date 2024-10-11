import datetime
import os
import requests
import psycopg2
from dotenv import load_dotenv

# Cargar el archivo .env
load_dotenv()

# Conexión a Redshift
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")

# Obtener las claves API desde el archivo .env
API_KEY = os.getenv("API_KEY")
API_HOST = os.getenv("API_HOST")

CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d").replace("-", "_")


# Función para extraer datos meteorológicos y cargarlos en Redshift


def extract_weather_data(locations):
    # Conexión a Redshift
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    cursor = conn.cursor()

    for location in locations:
        url = f"https://weatherapi-com.p.rapidapi.com/current.json?q={location}&lang=en"

        headers = {
            "X-RapidAPI-Key": API_KEY,
            "X-RapidAPI-Host": API_HOST
        }

        response = requests.get(url, headers=headers)
        data = response.json()

        # Extraer datos de JSON
        location_data = data['location']
        current_data = data['current']

        # Preparar los datos para insertarlos en Redshift
        insert_query = f"""
        INSERT INTO "{REDSHIFT_SCHEMA}".weather_staging (
            location_name, region, country, lat, lon, tz_id,
            localtime_epoch, local_time, last_updated_epoch,
            last_updated, temp_c, temp_f, is_day, condition_text,
            wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb,
            precip_mm, humidity, cloud, feelslike_c, feelslike_f,
            dewpoint_c, dewpoint_f, visibility_km, visibility_miles,
            uv_index, gust_mph, gust_kph, inserted_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, GETDATE()
        )
        """

        # Crear una tupla con los valores que serán insertados
        values = (
            location_data['name'], location_data['region'], location_data['country'],
            location_data['lat'], location_data['lon'], location_data['tz_id'],
            location_data['localtime_epoch'], location_data['localtime'],
            current_data['last_updated_epoch'], current_data['last_updated'],
            current_data['temp_c'], current_data['temp_f'], current_data['is_day'],
            current_data['condition']['text'], current_data['wind_mph'],
            current_data['wind_kph'], current_data['wind_degree'],
            current_data['wind_dir'], current_data['pressure_mb'],
            current_data['precip_mm'], current_data['humidity'],
            current_data['cloud'], current_data['feelslike_c'],
            current_data['feelslike_f'], current_data['dewpoint_c'],
            current_data['dewpoint_f'], current_data['vis_km'],
            current_data['vis_miles'], current_data['uv'],
            current_data['gust_mph'], current_data['gust_kph']
        )

        # Ejecutar la inserción en Redshift
        cursor.execute(insert_query, values)
        conn.commit()
        print(f"Datos meteorológicos para {location} insertados en Redshift.")

    cursor.close()
    conn.close()


locations = [
    "Cordoba", "Buenos Aires", "Rosario", "Ushuaia", "Montevideo", "Santiago",
    "Colonia del Sacramento", "Brasilia", "Rio de Janeiro", "Natal", "Oranjestad",
    "Willemstad", "Madrid", "Moscow", "Auckland", "Casablanca", "Sydney"
]

extract_weather_data(locations)
