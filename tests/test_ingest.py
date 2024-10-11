import os
import requests
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las claves API desde las variables de entorno
API_KEY = os.getenv("API_KEY")
API_HOST = os.getenv("API_HOST")

# Lista de ubicaciones para el test
locations = [
    "Cordoba", "Buenos Aires", "Rosario", "Ushuaia", "Montevideo", "Santiago",
    "Colonia del Sacramento", "Brasilia", "Rio de Janeiro", "Natal", "Oranjestad",
    "Willemstad", "Madrid", "Moscow", "Auckland", "Casablanca", "Sydney"
]

# Estructura de datos esperada basada en los tipos de la tabla en Redshift
expected_dtypes = {
    "location_name": str,
    "region": str,
    "country": str,
    "lat": float,
    "lon": float,
    "tz_id": str,
    "localtime_epoch": int,
    "local_time": str,
    "last_updated_epoch": int,
    "last_updated": str,
    "temp_c": float,
    "temp_f": float,
    "is_day": int,
    "condition_text": str,
    "wind_mph": float,
    "wind_kph": float,
    "wind_degree": int,
    "wind_dir": str,
    "pressure_mb": float,
    "precip_mm": float,
    "humidity": int,
    "cloud": int,
    "feelslike_c": float,
    "feelslike_f": float,
    "dewpoint_c": float,
    "dewpoint_f": float,
    "visibility_km": float,
    "visibility_miles": float,
    "uv_index": float,
    "gust_mph": float,
    "gust_kph": float
}


def test_api_fetch_for_all_locations():
    """
    Test para verificar que la API devuelve los datos para todas las ubicaciones
    y que los tipos de datos coinciden con los definidos en la tabla de Redshift.
    """
    for location in locations:
        url = f"https://weatherapi-com.p.rapidapi.com/current.json?q={location}&lang=en"

        headers = {
            "X-RapidAPI-Key": API_KEY,
            "X-RapidAPI-Host": API_HOST
        }

        response = requests.get(url, headers=headers)
        data = response.json()

        # Validar que los datos se recibieron correctamente
        assert response.status_code == 200, f"Error en la llamada para {location}"
        assert "location" in data and "current" in data, \
            f"Faltan datos en la respuesta para {location}"

        # Extraer datos
        location_data = data['location']
        current_data = data['current']

        # Comparar los tipos de datos
        actual_values = {
            "location_name": location_data['name'],
            "region": location_data['region'],
            "country": location_data['country'],
            "lat": location_data['lat'],
            "lon": location_data['lon'],
            "tz_id": location_data['tz_id'],
            "localtime_epoch": location_data['localtime_epoch'],
            "local_time": location_data['localtime'],
            "last_updated_epoch": current_data['last_updated_epoch'],
            "last_updated": current_data['last_updated'],
            "temp_c": current_data['temp_c'],
            "temp_f": current_data['temp_f'],
            "is_day": current_data['is_day'],
            "condition_text": current_data['condition']['text'],
            "wind_mph": current_data['wind_mph'],
            "wind_kph": current_data['wind_kph'],
            "wind_degree": current_data['wind_degree'],
            "wind_dir": current_data['wind_dir'],
            "pressure_mb": current_data['pressure_mb'],
            "precip_mm": current_data['precip_mm'],
            "humidity": current_data['humidity'],
            "cloud": current_data['cloud'],
            "feelslike_c": current_data['feelslike_c'],
            "feelslike_f": current_data['feelslike_f'],
            "dewpoint_c": current_data['dewpoint_c'],
            "dewpoint_f": current_data['dewpoint_f'],
            "visibility_km": current_data['vis_km'],
            "visibility_miles": current_data['vis_miles'],
            "uv_index": current_data['uv'],
            "gust_mph": current_data['gust_mph'],
            "gust_kph": current_data['gust_kph']
        }

        # Verificar que los tipos de datos coinciden con los esperados
        for field, expected_type in expected_dtypes.items():
            actual_value = actual_values[field]
            assert isinstance(actual_value, expected_type), \
                f"""Error de tipo en {field} para {location}:
                se esperaba {expected_type}, pero se obtuvo {type(actual_value)}"""
        print(f"Todos los tipos de datos para {location} coinciden con los esperados.")


# Ejecutar el test


test_api_fetch_for_all_locations()
