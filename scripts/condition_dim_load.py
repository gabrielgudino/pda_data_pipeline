import os
import psycopg2
from dotenv import load_dotenv


# Cargar variables de entorno desde el archivo .env
load_dotenv()

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
conn.commit()
print(f"Esquema establecido a: {REDSHIFT_SCHEMA}")


# Función para obtener las condiciones climáticas desde la tabla de staging


def get_conditions_from_staging():
    conditions = set()  # Usamos un set para evitar duplicados

    # Consulta para obtener las condiciones insertadas en los últimos 30 minutos
    query = """
    SELECT DISTINCT condition_text, condition_icon, condition_code
    FROM weather_staging
    WHERE created_at >= dateadd(minute, -30, GETDATE());
    """

    cursor.execute(query)
    results = cursor.fetchall()

    # Añadir las condiciones al set
    for row in results:
        condition_text = row[0]
        condition_icon = row[1]
        condition_code = row[2]

        if condition_text and condition_icon and condition_code:
            conditions.add((condition_text, condition_icon, condition_code))

    return conditions


# Función para insertar condiciones en la tabla condition_dim


def insert_conditions_into_condition_dim(conditions):
    for condition_text, condition_icon, condition_code in conditions:
        # Verificar si la condición ya existe en la tabla condition_dim
        cursor.execute(
            "SELECT condition_id FROM condition_dim WHERE condition_code = %s",
            (condition_code,)
        )
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
    conditions = get_conditions_from_staging()
    if conditions:
        insert_conditions_into_condition_dim(conditions)
        print("Proceso de carga de condiciones completado.")
    else:
        print("No se encontraron nuevas condiciones en los últimos 30 minutos.")

# Cerrar la conexión
cursor.close()
conn.close()
