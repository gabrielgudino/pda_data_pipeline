import datetime
import os
from dotenv import load_dotenv
import requests
import json
import pandas as pd
import boto3


load_dotenv()

CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d").replace("-","_")
WORK_DIR = os.getenv("WORK_DIR")
S3_BUCKET = os.getenv("AWS_S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")


WORK_DIR = os.getenv("WORK_DIR")

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

# Inicializamos el cliente de S3
s3_client = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

def upload_to_s3():
    """
    Función que sube todos los archivos en el directorio correspondiente a la fecha actual
    al bucket de S3, manteniendo la estructura de directorios por fecha.
    """
    local_directory = f"{WORK_DIR}/data/{CURRENT_DATE}"

    # Recorrer todos los archivos en el directorio correspondiente a la fecha
    for filename in os.listdir(local_directory):
        file_path = os.path.join(local_directory, filename)
        s3_key = f"data/{CURRENT_DATE}/{filename}"  # Ruta en el bucket de S3
        
        try:
            s3_client.upload_file(file_path, S3_BUCKET, s3_key)
            print(f"Archivo {file_path} subido exitosamente a s3://{S3_BUCKET}/{s3_key}")
        except Exception as e:
            print(f"Error al subir {file_path} a S3: {e}")

locations = ["Cordoba", "Buenos Aires", "Rosario", "Ushuaia", "Montevideo", "Santiago", "Colonia del Sacramento", "Brasilia", "Rio de Janeiro", "Natal", "Oranjestad", "Willemstad", "Madrid", "Moscow", 
             "Auckland", "Casablanca", "Sydney"]

extract_weather_data(locations)