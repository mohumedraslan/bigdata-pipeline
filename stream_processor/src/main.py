import os
import json
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType, TimestampType,
)

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("stream-processor")

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_RAW       = os.getenv("KAFKA_TOPIC_RAW", "parking.sensors.raw")
INFLUX_URL      = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN    = os.getenv("INFLUX_TOKEN", "super-secret-token-change-me")
INFLUX_ORG      = os.getenv("INFLUX_ORG", "smart_parking")
INFLUX_BUCKET   = os.getenv("INFLUX_BUCKET", "sensor_data")
MONGO_URI       = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB        = os.getenv("MONGO_DB", "smart_parking")
SPARK_JARS_PACKAGES = os.getenv(
    "SPARK_JARS_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
)

SENSOR_SCHEMA = StructType([
    StructField("unit_id",      StringType(),  nullable=False),
    StructField("spot_id",      StringType(),  nullable=False),
    StructField("session_id",   StringType(),  nullable=False),
    StructField("phase",        StringType(),  nullable=False),
    StructField("front_cm",     FloatType(),   nullable=False),
    StructField("rear_cm",      FloatType(),   nullable=False),
    StructField("left_cm",      FloatType(),   nullable=False),
    StructField("right_cm",     FloatType(),   nullable=False),
    StructField("servo_angle",  IntegerType(), nullable=False),
    StructField("speed_ms",     FloatType(),   nullable=False),
    StructField("ts",           StringType(),  nullable=False),
])


def write_to_influx(df: DataFrame, epoch_id: int) -> None:
    rows = df.collect()
    if not rows:
        return

    client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    points    = []

    for row in rows:
        p = (
            Point("sensor_reading")
            .tag("unit_id",    row["unit_id"])
            .tag("spot_id",    row["spot_id"])
            .tag("phase",      row["phase"])
            .tag("session_id", row["session_id"])
            .field("front_cm",    float(row["front_cm"]))
            .field("rear_cm",     float(row["rear_cm"]))
            .field("left_cm",     float(row["left_cm"]))
            .field("right_cm",   float(row["right_cm"]))
            .field("servo_angle", int(row["servo_angle"]))
            .field("speed_ms",    float(row["speed_ms"]))
            .field("min_clearance", float(min(
                row["front_cm"], row["rear_cm"],
                row["left_cm"],  row["right_cm"]
            )))
            .time(row["ts"], WritePrecision.NS)
        )
        points.append(p)

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
    write_api.close()
    client.close()
    log.info("[InfluxDB] epoch=%d  written=%d points", epoch_id, len(points))


def write_to_mongo(df: DataFrame, epoch_id: int) -> None:
    rows = df.collect()
    if not rows:
        return

    client = MongoClient(MONGO_URI)
    col    = client[MONGO_DB]["sensor_events"]
    docs   = [row.asDict() for row in rows]
    col.insert_many(docs)
    client.close()
    log.info("[MongoDB] epoch=%d  inserted=%d docs", epoch_id, len(docs))


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("SmartParking-StreamProcessor")
        .config("spark.jars.packages", SPARK_JARS_PACKAGES)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("d"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("kafka_key", "kafka_ts", "d.*")
        .withColumn("event_time", F.to_timestamp("ts"))
        .withColumn(
            "min_clearance",
            F.least("front_cm", "rear_cm", "left_cm", "right_cm")
        )
        .withColumn(
            "is_critical",
            F.col("min_clearance") < F.lit(15.0)
        )
        .withColumnRenamed("is_critical", "alert_flag")
    )

    sink_influx = (
        parsed.writeStream
        .foreachBatch(write_to_influx)
        .option("checkpointLocation", "/tmp/checkpoints/influx")
        .trigger(processingTime="2 seconds")
        .start()
    )

    sink_mongo = (
        parsed.writeStream
        .foreachBatch(write_to_mongo)
        .option("checkpointLocation", "/tmp/checkpoints/mongo")
        .trigger(processingTime="2 seconds")
        .start()
    )

    windowed = (
        parsed
        .withWatermark("event_time", "10 seconds")
        .groupBy(
            F.window("event_time", "30 seconds"),
            "unit_id", "spot_id", "phase",
        )
        .agg(
            F.count("*")            .alias("reading_count"),
            F.avg("front_cm")       .alias("avg_front_cm"),
            F.avg("rear_cm")        .alias("avg_rear_cm"),
            F.min("min_clearance")  .alias("min_clearance_30s"),
            F.sum(
                F.when(F.col("alert_flag"), 1).otherwise(0)
            ).alias("alert_count"),
        )
    )

    sink_window = (
        windowed.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", "/tmp/checkpoints/window")
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("All streaming sinks started — awaiting data …")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
            .field("min_clearance", float(min(
                row["front_cm"], row["rear_cm"],
                row["left_cm"],  row["right_cm"]
            )))
            .time(row["ts"], WritePrecision.NS)
        )
        points.append(p)

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
    write_api.close()
    client.close()
    log.info("[InfluxDB] epoch=%d  written=%d points", epoch_id, len(points))


def write_to_mongo(df: DataFrame, epoch_id: int) -> None:
    rows = df.collect()
    if not rows:
        return

    client = MongoClient(MONGO_URI)
    col    = client[MONGO_DB]["sensor_events"]
    docs   = [row.asDict() for row in rows]
    col.insert_many(docs)
    client.close()
    log.info("[MongoDB] epoch=%d  inserted=%d docs", epoch_id, len(docs))


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("SmartParking-StreamProcessor")
        .config("spark.jars.packages", SPARK_JARS_PACKAGES)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("d"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("kafka_key", "kafka_ts", "d.*")
        .withColumn("event_time", F.to_timestamp("ts"))
        .withColumn(
            "min_clearance",
            F.least("front_cm", "rear_cm", "left_cm", "right_cm")
        )
        .withColumn(
            "is_critical",
            F.col("min_clearance") < F.lit(15.0)
        )
        .withColumnRenamed("is_critical", "alert_flag")
    )

    sink_influx = (
        parsed.writeStream
        .foreachBatch(write_to_influx)
        .option("checkpointLocation", "/tmp/checkpoints/influx")
        .trigger(processingTime="2 seconds")
        .start()
    )

    sink_mongo = (
        parsed.writeStream
        .foreachBatch(write_to_mongo)
        .option("checkpointLocation", "/tmp/checkpoints/mongo")
        .trigger(processingTime="2 seconds")
        .start()
    )

    windowed = (
        parsed
        .withWatermark("event_time", "10 seconds")
        .groupBy(
            F.window("event_time", "30 seconds"),
            "unit_id", "spot_id", "phase",
        )
        .agg(
            F.count("*")            .alias("reading_count"),
            F.avg("front_cm")       .alias("avg_front_cm"),
            F.avg("rear_cm")        .alias("avg_rear_cm"),
            F.min("min_clearance")  .alias("min_clearance_30s"),
            F.sum(
                F.when(F.col("alert_flag"), 1).otherwise(0)
            ).alias("alert_count"),
        )
    )

    sink_window = (
        windowed.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", "/tmp/checkpoints/window")
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("All streaming sinks started — awaiting data …")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
        )
        .agg(
            F.count("*")            .alias("reading_count"),
            F.avg("front_cm")       .alias("avg_front_cm"),
            F.avg("rear_cm")        .alias("avg_rear_cm"),
            F.min("min_clearance")  .alias("min_clearance_30s"),
            F.sum(
                F.when(F.col("alert_flag"), 1).otherwise(0)
            ).alias("alert_count"),
        )
    )

    sink_window = (
        windowed.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", "/tmp/checkpoints/window")
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("✅ All streaming sinks started — awaiting data …")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
