import os
import psycopg2
from dotenv import load_dotenv
import datetime

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


# Función para cargar los datos de weather_staging a weather_fact
def load_weather_data():
    # Consulta para obtener los registros insertados en los últimos 30 minutos
    query = """
    SELECT location_name, condition_code, last_updated_epoch, temp_c, temp_f, is_day,
           wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in,
           precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c,
           windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, visibility_km,
           visibility_miles, uv_index, gust_mph, gust_kph
    FROM weather_staging
    WHERE created_at >= dateadd(minute, -30, GETDATE());
    """

    cursor.execute(query)
    results = cursor.fetchall()

    for row in results:
        (location_name, condition_code, last_updated_epoch, temp_c, temp_f, is_day,
         wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in,
         precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c,
         windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km,
         vis_miles, uv, gust_mph, gust_kph) = row

        # Convertir last_updated_epoch a fecha
        last_updated_date = datetime.datetime.\
            utcfromtimestamp(last_updated_epoch).date()

        # Obtener el date_id desde la tabla date_dim
        cursor.execute("""
                       SELECT date_id FROM date_dim WHERE date = %s
                       """, (last_updated_date,))
        date_result = cursor.fetchone()
        if date_result:
            date_id = date_result[0]
        else:
            print(f"No se encontró un date_id para la fecha {last_updated_date}. "
                  "Saltando registro.")
            continue

        # Obtener location_id desde location_dim
        cursor.execute("""
                       SELECT location_id FROM location_dim WHERE location_name = %s
                       """, (location_name,))
        location_result = cursor.fetchone()
        if location_result:
            location_id = location_result[0]
        else:
            print(f"No se encontró un location_id para la ubicación {location_name}. "
                  "Saltando registro.")
            continue

        # Obtener condition_id desde condition_dim
        cursor.execute("""
                       SELECT condition_id FROM condition_dim WHERE condition_code = %s
                       """, (condition_code,))
        condition_result = cursor.fetchone()
        if condition_result:
            condition_id = condition_result[0]
        else:
            print(f"No se encontró un condition_id para el código {condition_code}. "
                  "Saltando registro.")
            continue

        # Insertar en weather_fact
        cursor.execute("""
            INSERT INTO weather_fact (location_id, condition_id, date_id,
            temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree,
            wind_dir, pressure_mb, pressure_in, precip_mm, precip_in,
            humidity, cloud, feelslike_c, feelslike_f, windchill_c,
            windchill_f, heatindex_c, heatindex_f, dewpoint_c,
            dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            location_id, condition_id, date_id, temp_c, temp_f, is_day,
            wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb,
            pressure_in, precip_mm, precip_in, humidity, cloud,
            feelslike_c, feelslike_f, windchill_c, windchill_f,
            heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km,
            vis_miles, uv, gust_mph, gust_kph
        ))

    conn.commit()


if __name__ == "__main__":
    load_weather_data()
    print("Datos meteorológicos cargados en weather_fact exitosamente.")

# Cerrar el cursor y la conexión
cursor.close()
conn.close()
