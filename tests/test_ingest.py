import os
from unittest.mock import patch, Mock
from dotenv import load_dotenv
from scripts.ingest import extract_weather_data


# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las claves API desde las variables de entorno
API_KEY = os.getenv("API_KEY")
API_HOST = os.getenv("API_HOST")


@patch('psycopg2.connect')  # Simular la conexión a Redshift
@patch('requests.get')  # Simular la solicitud a la API de WeatherAPI
def test_extract_weather_data(mock_get, mock_connect):
    # Simular una respuesta exitosa de la API
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "location": {
            "name": "Cordoba",
            "region": "Cordoba",
            "country": "Argentina",
            "lat": -31.4,
            "lon": -64.18,
            "tz_id": "America/Argentina/Cordoba",
            "localtime_epoch": 1727728265,
            "localtime": "2024-10-01 09:31"
        },
        "current": {
            "last_updated_epoch": 1727727300,
            "last_updated": "2024-10-01 09:15",
            "temp_c": 20.5,
            "temp_f": 68.9,
            "is_day": 1,
            "condition": {"text": "Sunny"},
            "wind_mph": 10.4,
            "wind_kph": 16.7,
            "wind_degree": 90,
            "wind_dir": "E",
            "pressure_mb": 1015.0,
            "precip_mm": 0.0,
            "humidity": 56,
            "cloud": 0,
            "feelslike_c": 20.5,
            "feelslike_f": 68.9,
            "dewpoint_c": 11.5,
            "dewpoint_f": 52.7,
            "vis_km": 10.0,
            "vis_miles": 6.0,
            "uv": 7.0,
            "gust_mph": 15.0,
            "gust_kph": 24.1
        }
    }
    mock_get.return_value = mock_response

    # Simular la conexión a Redshift
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Llamar a la función con una ubicación simulada
    extract_weather_data(['Cordoba'])

    # Verificar que la API fue llamada correctamente
    mock_get.assert_called_once_with(
        "https://weatherapi-com.p.rapidapi.com/current.json?q=Cordoba&lang=en",
        headers={'X-RapidAPI-Key': API_KEY, 'X-RapidAPI-Host': API_HOST}
    )

    # Verificar que los valores de la consulta SQL son correctos
    expected_values = (
        "Cordoba", "Cordoba", "Argentina", -31.4, -64.18, "America/Argentina/Cordoba",
        1727728265, "2024-10-01 09:31", 1727727300, "2024-10-01 09:15", 20.5, 68.9, 1,
        "Sunny", 10.4, 16.7, 90, "E", 1015.0, 0.0, 56, 0, 20.5, 68.9, 11.5, 52.7, 10.0,
        6.0, 7.0, 15.0, 24.1
    )

    # Verificar que los valores de la consulta SQL son correctos
    called_args = mock_cursor.execute.call_args
    assert called_args[0][1] == expected_values, \
        "Los valores de la consulta no coinciden"
