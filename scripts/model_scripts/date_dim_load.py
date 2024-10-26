import psycopg2
import datetime
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

# Conectar a Redshift usando psycopg2
conn = psycopg2.connect(
    host=os.getenv("REDSHIFT_HOST"),
    port=os.getenv("REDSHIFT_PORT", "5439"),
    dbname=os.getenv("REDSHIFT_DB"),
    user=os.getenv("REDSHIFT_USER"),
    password=os.getenv("REDSHIFT_PASSWORD")
)
cursor = conn.cursor()

# Configurar el esquema
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")
cursor.execute(f'SET search_path TO "{REDSHIFT_SCHEMA}";')
conn.commit()


# Función para generar los datos de la dimensión de fecha (date_dim)
def generate_date_data(start_date, end_date):
    date_data = []
    current_date = start_date
    while current_date <= end_date:
        day_of_week = current_date.weekday() + 1  # Devuelve 0-6 (0=Lun, 6=Dom)
        is_weekend = 1 if day_of_week >= 6 else 0  # Sábado (6) o Domingo (7)

        date_data.append({
            'date': current_date,
            'day': current_date.day,
            'day_of_week': day_of_week,
            'day_name': current_date.strftime('%A'),  # Nombre del día
            'week': current_date.isocalendar()[1],  # Semana del año
            'month': current_date.month,
            'month_name': current_date.strftime('%B'),  # Nombre del mes
            'quarter': (current_date.month - 1) // 3 + 1,  # Trimestre del año
            'year': current_date.year,
            'is_weekend': is_weekend,
            'created_at': datetime.datetime.now()  # Fecha de creación
        })
        current_date += datetime.timedelta(days=1)
    return date_data


# Función para verificar si la tabla está vacía
def is_table_empty():
    cursor.execute("SELECT COUNT(*) FROM date_dim;")
    result = cursor.fetchone()
    return result[0] == 0


# Función para insertar los datos en la tabla date_dim
def insert_date_data(date_data):
    if is_table_empty():
        for date_record in date_data:
            cursor.execute("""
                INSERT INTO date_dim (date, day, day_of_week, day_name,
                week, month, month_name, quarter, year, is_weekend, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                date_record['date'],
                date_record['day'],
                date_record['day_of_week'],
                date_record['day_name'],
                date_record['week'],
                date_record['month'],
                date_record['month_name'],
                date_record['quarter'],
                date_record['year'],
                date_record['is_weekend'],
                date_record['created_at']
            ))
        conn.commit()
        print("Datos de la dimensión de fecha cargados exitosamente.")
    else:
        print("La tabla date_dim ya contiene datos. No se realizó ninguna carga.")


# Parámetros de tiempo: desde enero 2024 hasta diciembre 2025
start_date = datetime.datetime(2024, 1, 1)
end_date = datetime.datetime(2024, 12, 31)

# Generar los datos de la dimensión de fecha
date_data = generate_date_data(start_date, end_date)

# Insertar los datos en la tabla date_dim
insert_date_data(date_data)

# Cerrar la conexión
cursor.close()
conn.close()
