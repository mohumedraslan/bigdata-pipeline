FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jdk-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"

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
