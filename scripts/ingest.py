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


# Función de extracción de datos meteorológicos
def extract_weather_data(location):
    url = f"https://weatherapi-com.p.rapidapi.com/current.json?q={location}&lang=en"
    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": API_HOST
    }

    response = requests.get(url, headers=headers)
    data = response.json()

    # Verificar si 'location' y 'current' existen en la respuesta
    location_data = data['location']
    current_data = data['current']

    # Crear un diccionario con los datos relevantes
    extracted_data = (
        location_data['name'], location_data['region'], location_data['country'],
        location_data['lat'], location_data['lon'], location_data['tz_id'],
        location_data['localtime_epoch'], location_data['localtime'],
        current_data['last_updated_epoch'], current_data['last_updated'],
        current_data['temp_c'], current_data['temp_f'], current_data['is_day'],
        current_data['condition']['text'], current_data['condition']['icon'],
        current_data['condition']['code'],
        current_data['wind_mph'], current_data['wind_kph'], current_data['wind_degree'],
        current_data['wind_dir'], current_data['pressure_mb'],
        current_data['pressure_in'], current_data['precip_mm'],
        current_data['precip_in'], current_data['humidity'], current_data['cloud'],
        current_data['feelslike_c'], current_data['feelslike_f'],
        current_data['windchill_c'], current_data['windchill_f'],
        current_data['heatindex_c'], current_data['heatindex_f'],
        current_data['dewpoint_c'], current_data['dewpoint_f'],
        current_data['vis_km'], current_data['vis_miles'],
        current_data['uv'], current_data['gust_mph'],
        current_data['gust_kph']
    )

    return extracted_data


# Función de carga de datos en Redshift
def load_weather_data_to_redshift(extracted_data):
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    cursor = conn.cursor()

    insert_query = f"""
    INSERT INTO "{REDSHIFT_SCHEMA}".weather_staging (
    location_name, region, country, lat, lon, tz_id,
    localtime_epoch, local_time, last_updated_epoch,
    last_updated, temp_c, temp_f, is_day, condition_text,
    condition_icon, condition_code, wind_mph, wind_kph, wind_degree,
    wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud,
    feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f,
    dewpoint_c, dewpoint_f, visibility_km, visibility_miles, uv_index,
    gust_mph, gust_kph, created_at)
    VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
    );
    """

    cursor.execute(insert_query, extracted_data)
    conn.commit()
    cursor.close()
    conn.close()


# Función que ejecuta todo el proceso


def run_etl(locations):
    for location in locations:
        extracted_data = extract_weather_data(location)
        load_weather_data_to_redshift(extracted_data)
        print(f"Datos meteorológicos para {location} procesados.")


if __name__ == "__main__":
    # Lista de ubicaciones
    locations = [
        "Cordoba", "Buenos Aires", "Rosario", "Ushuaia", "Montevideo", "Santiago",
        "Colonia del Sacramento", "Brasilia", "Rio de Janeiro", "Natal", "Oranjestad",
        "Willemstad", "Madrid", "Moscow", "Auckland", "Casablanca", "Sydney"
    ]
    # Ejecutar el ETL
    run_etl(locations)
