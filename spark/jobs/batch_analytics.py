"""
spark/jobs/batch_analytics.py
───────────────────────────────
Batch job that runs on a schedule (e.g. nightly) against the MongoDB
event history to produce summary statistics and session-level KPIs.

Metrics computed
────────────────
1. Session duration (time from APPROACHING → PARKED) per truck
2. Alert frequency per spot (identifies dangerous spots)
3. Average clearance by phase (validates sensor calibration)
4. Phase transition counts (distribution of parking difficulty)

In production you'd schedule this with Airflow / Spark on YARN.
For the university demo: run manually or set restart: "no" in compose.
"""

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("batch-analytics")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
MONGO_DB  = os.getenv("MONGO_DB", "smart_parking")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("SmartParking-BatchAnalytics")
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        )
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Load full event history ────────────────────────────────────────────
    df = (
        spark.read
        .format("mongodb")
        .option("database", MONGO_DB)
        .option("collection", "sensor_events")
        .load()
        .withColumn("event_time", F.to_timestamp("ts"))
    )

    total = df.count()
    log.info("📦 Loaded %d sensor events from MongoDB", total)

    if total == 0:
        log.warning("No data yet — run the simulator first.")
        return

    # ── 1. Session KPIs ───────────────────────────────────────────────────
    session_kpis = (
        df.groupBy("session_id", "unit_id", "spot_id")
        .agg(
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
            F.count("*").alias("total_readings"),
            F.min(F.least("front_cm", "rear_cm", "left_cm", "right_cm"))
                .alias("global_min_clearance"),
            F.avg("speed_ms").alias("avg_speed_ms"),
        )
        .withColumn(
            "duration_seconds",
            F.unix_timestamp("session_end") - F.unix_timestamp("session_start"),
        )
    )

    print("\n" + "═" * 70)
    print("  SESSION KPIs")
    print("═" * 70)
    session_kpis.orderBy("session_start", ascending=False).show(20, truncate=False)

    # ── 2. Alert frequency by parking spot ────────────────────────────────
    alert_df = (
        spark.read
        .format("mongodb")
        .option("database", MONGO_DB)
        .option("collection", "alerts")
        .load()
    )

    if alert_df.count() > 0:
        hot_spots = (
            alert_df
            .groupBy("spot_id" if "spot_id" in alert_df.columns else F.lit("unknown"))
            .agg(F.count("*").alias("alert_count"))
            .orderBy("alert_count", ascending=False)
        )
        print("\n" + "═" * 70)
        print("  ALERT FREQUENCY BY SPOT  (higher = more dangerous)")
        print("═" * 70)
        hot_spots.show(10, truncate=False)

    # ── 3. Clearance statistics by phase ──────────────────────────────────
    phase_stats = (
        df.groupBy("phase")
        .agg(
            F.avg("front_cm").alias("avg_front"),
            F.avg("rear_cm").alias("avg_rear"),
            F.avg(F.least("front_cm", "rear_cm", "left_cm", "right_cm"))
                .alias("avg_min_clearance"),
            F.count("*").alias("reading_count"),
        )
        .orderBy("avg_min_clearance")
    )

    print("\n" + "═" * 70)
    print("  CLEARANCE STATS BY PHASE")
    print("═" * 70)
    phase_stats.show(truncate=False)

    # ── 4. Phase distribution ─────────────────────────────────────────────
    phase_dist = (
        df.groupBy("unit_id", "phase")
        .agg(F.count("*").alias("ticks"))
        .orderBy("unit_id", "ticks", ascending=[True, False])
    )

    print("\n" + "═" * 70)
    print("  PHASE DISTRIBUTION PER UNIT  (how many ticks in each phase)")
    print("═" * 70)
    phase_dist.show(30, truncate=False)

    spark.stop()
    log.info("✅ Batch analytics complete")


if __name__ == "__main__":
    main()
