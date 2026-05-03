FROM bitnami/spark:3.5.1

USER root
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY jobs/ ./jobs/

ENV PYSPARK_PYTHON=python3

WORKDIR /app/jobs
CMD ["python", "batch_analytics.py"]
