FROM bitnami/spark:3.5.1

USER root

WORKDIR /app

# ── Python dependencies ──────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# ── Tell Spark to download Kafka connector at startup ────────────────────────
ENV PYSPARK_PYTHON=python3
ENV SPARK_JARS_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

WORKDIR /app/src
CMD ["python", "main.py"]
