import datetime
import os
from dotenv import load_dotenv
import requests
import json
import pandas as pd
# import boto3


# load_dotenv()

CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d").replace("-","_")


WORK_DIR = os.getenv("WORK_DIR", "/opt/airflow")

def extract_weather_data(locations):
    """
    Función que extrae información meteorológica actual para una lista de ubicaciones
    utilizando la API de WeatherAPI. 

    Parámetros:
    - locations (list): Lista de ubicaciones para las cuales se desea obtener la información del clima.

    La función hace lo siguiente:
    1. Verifica si existe un directorio con la fecha actual en la carpeta "data", y si no existe, lo crea.
    2. Para cada ubicación en la lista, realiza una solicitud a la API para obtener la información climática.
    3. Guarda la respuesta de la API en un archivo de texto (.txt) con el nombre de la ubicación en la carpeta creada.
    """
    if not os.path.exists(f"{WORK_DIR}/data/{CURRENT_DATE}"):
        os.mkdir(f"{WORK_DIR}/data/{CURRENT_DATE}")

    for location in locations:
        url = "https://weatherapi-com.p.rapidapi.com/current.json?q=" + location + "&lang=en"

        headers = {
            "X-RapidAPI-Key": "24cc538b51msh9dd38f0d1f4fd7ap150793jsn82c69f528d4e",
            "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers)

        print(response.json())

        # Generamos los archivos de texto
        with open(f"{WORK_DIR}/data/{CURRENT_DATE}/{location}.txt", "w") as write_file:
            write_file.write(json.dumps(response.json()))
            write_file.close()


locations = ["Cordoba", "Buenos Aires", "Rosario", "Ushuaia", "Montevideo", "Santiago", "Colonia del Sacramento", "Brasilia", "Rio de Janeiro", "Natal", "Oranjestad", "Willemstad", "Madrid", "Moscow", 
             "Auckland", "Casablanca", "Sydney"]

extract_weather_data(locations)