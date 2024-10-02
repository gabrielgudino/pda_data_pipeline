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

# Función para extraer las condiciones climáticas de los archivos
def get_conditions_from_files():
    conditions = set()  # Usamos un set para evitar duplicados

    # Recorrer todos los archivos .txt en el directorio
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.txt'):
            file_path = os.path.join(DATA_DIR, filename)
            
            # Abrir y leer el archivo
            with open(file_path, 'r') as file:
                data = json.load(file)
                condition_text = data['current']['condition']['text']
                condition_icon = data['current']['condition']['icon']
                condition_code = data['current']['condition']['code']
                
                # Añadir la condición si los campos no están vacíos
                if condition_text and condition_icon and condition_code:
                    conditions.add((condition_text, condition_icon, condition_code))

    return conditions

# Función para insertar condiciones en la tabla condition_dim
def insert_conditions_into_condition_dim(conditions):
    for condition_text, condition_icon, condition_code in conditions:
        # Verificar si la condición ya existe en la tabla condition_dim
        cursor.execute("SELECT condition_id FROM condition_dim WHERE condition_code = %s", (condition_code,))
        condition_result = cursor.fetchone()

        # Si la condición no existe, insertarla
        if condition_result is None:
            cursor.execute("""
                INSERT INTO condition_dim (condition_text, condition_icon, condition_code)
                VALUES (%s, %s, %s)
            """, (condition_text, condition_icon, condition_code))
            print(f"Condición insertada: {condition_text} (Código: {condition_code})")
        else:
            print(f"Condición ya existente: {condition_text} (Código: {condition_code})")

    # Hacer commit para guardar los cambios
    conn.commit()

if __name__ == "__main__":
    conditions = get_conditions_from_files()
    insert_conditions_into_condition_dim(conditions)
    print("Proceso de carga de condiciones completado.")
    
# Cerrar la conexión
cursor.close()
conn.close()
