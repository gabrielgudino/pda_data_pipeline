FROM apache/airflow:slim-latest-python3.9

USER root

# Instalar las dependencias necesarias
RUN apt-get update && apt-get install -y \
    libpq-dev gcc build-essential

USER airflow

WORKDIR /opt/airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["airflow", "webserver"]
