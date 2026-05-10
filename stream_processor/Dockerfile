FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

CMD ["python", "src/main.py"]
ENV SPARK_JARS_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

WORKDIR /app/src
CMD ["python", "main.py"]
