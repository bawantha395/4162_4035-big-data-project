"""
spark_fraud_detection.py
═══════════════════════════════════════════════════════════════════
FinTech Fraud Detection Pipeline — Spark Structured Streaming Job

Consumes the 'transactions_topic' from Kafka, applies two fraud
detection rules in real-time, and writes results to PostgreSQL:

  Rule 1 — IMPOSSIBLE TRAVEL
      Same user_id appears in two different countries within a
      10-minute watermark window.

  Rule 2 — HIGH-VALUE TRANSACTION
      transaction amount > $5,000

Outputs:
  • fraud_alerts   table  ← flagged records (immediate)
  • transactions   table  ← all records with is_fraud flag

Submit:
  spark-submit \
    --master local[2] \
    --jars /opt/spark/jars/kafka-clients-*.jar,\
        /opt/spark/jars/spark-sql-kafka-0-10_*.jar,\
        /opt/spark/jars/postgresql-*.jar,\
        /opt/spark/jars/commons-pool2-*.jar \
    /opt/spark/jobs/spark_fraud_detection.py
═══════════════════════════════════════════════════════════════════
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType,
)

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("spark_fraud_detection")

# ── Configuration ─────────────────────────────────────────────
KAFKA_BROKERS         = "kafka_broker_1:19092,kafka_broker_2:19093,kafka_broker_3:19094"
TRANSACTION_TOPIC     = "transactions_topic"
POSTGRES_URL          = "jdbc:postgresql://fraud_postgres:5432/fraud_detection"
POSTGRES_PROPS        = {
    "user":     "fraud_admin",
    "password": "fraud_secure_pass",
    "driver":   "org.postgresql.Driver",
}
CHECKPOINT_BASE       = "/tmp/fraud_checkpoints"
FRAUD_AMOUNT_THRESHOLD = 5000.0
TRAVEL_WINDOW_MINUTES  = 10


# ── Schema for incoming JSON ──────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),    True),
    StructField("user_id",           StringType(),    False),
    StructField("timestamp",         StringType(),    False),
    StructField("merchant_category", StringType(),    False),
    StructField("amount",            DoubleType(),    False),
    StructField("location_country",  StringType(),    False),
    StructField("location_city",     StringType(),    False),
])


# ── Spark session ─────────────────────────────────────────────
def build_spark_session():
    spark = (
        SparkSession.builder
        .appName("FraudDetectionPipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("✅  Spark session initialised — FraudDetectionPipeline")
    return spark


# ── Read stream from Kafka ────────────────────────────────────
def read_kafka_stream(spark):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TRANSACTION_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
        .select(
            F.from_json(F.col("json_str"), TRANSACTION_SCHEMA).alias("txn"),
            F.col("kafka_ts"),
        )
        .select("txn.*", "kafka_ts")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .withWatermark("event_time", f"{TRAVEL_WINDOW_MINUTES + 2} minutes")
    )

    log.info("✅  Kafka stream reader configured — topic: %s", TRANSACTION_TOPIC)
    return parsed


# ── Rule 2: High-value detection (stateless — per row) ────────
def detect_high_value(df):
    """
    Flag any single transaction exceeding FRAUD_AMOUNT_THRESHOLD.
    Returns a DataFrame with only fraud rows + fraud_reason column.
    """
    return (
        df
        .filter(F.col("amount") > FRAUD_AMOUNT_THRESHOLD)
        .withColumn("fraud_reason", F.lit(f"HIGH_VALUE > ${FRAUD_AMOUNT_THRESHOLD:.0f}"))
    )


# ── Rule 1: Impossible travel detection (stateful — windowed) ─
def detect_impossible_travel(df):
    """
    Within a TRAVEL_WINDOW_MINUTES tumbling window, find users who
    appear in more than one distinct country.

    Strategy: self-join on user_id within the watermarked window,
    where the two rows have different countries.
    """
    windowed = (
        df
        .groupBy(
            F.col("user_id"),
            F.window(F.col("event_time"), f"{TRAVEL_WINDOW_MINUTES} minutes"),
        )
        .agg(
            F.collect_set("location_country").alias("countries"),
            F.max("amount").alias("amount"),
            F.last("merchant_category").alias("merchant_category"),
            F.last("location_country").alias("location_country"),
            F.last("location_city").alias("location_city"),
            F.last("timestamp").alias("timestamp"),
            F.last("transaction_id").alias("transaction_id"),
        )
        .filter(F.size("countries") > 1)
        .withColumn(
            "fraud_reason",
            F.concat(
                F.lit("IMPOSSIBLE_TRAVEL: "),
                F.array_join(F.col("countries"), " → "),
            ),
        )
        .select(
            F.col("user_id"),
            F.col("timestamp"),
            F.col("merchant_category"),
            F.col("amount"),
            F.col("location_country"),
            F.col("location_city"),
            F.col("transaction_id"),
            F.col("fraud_reason"),
        )
    )
    return windowed


# ── JDBC write helper ─────────────────────────────────────────
def write_to_postgres(batch_df, batch_id, table):
    """
    foreachBatch writer: saves the micro-batch DataFrame to PostgreSQL.
    Uses 'append' mode — all fraud rows are preserved for audit.
    """
    if batch_df.isEmpty():
        return

    count = batch_df.count()
    log.info("✍️   Batch #%d → writing %d rows to [%s]", batch_id, count, table)

    (
        batch_df
        .write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table)
        .option("user",     POSTGRES_PROPS["user"])
        .option("password", POSTGRES_PROPS["password"])
        .option("driver",   POSTGRES_PROPS["driver"])
        .mode("append")
        .save()
    )
    log.info("✅  Batch #%d committed to [%s]", batch_id, table)


# ── Write ALL transactions (with is_fraud flag) ───────────────
def write_all_transactions(batch_df, batch_id):
    """
    Write every transaction to the 'transactions' table, marking
    fraud rows with is_fraud = True.
    """
    if batch_df.isEmpty():
        return

    # High-value fraud IDs
    fraud_ids = (
        batch_df
        .filter(F.col("amount") > FRAUD_AMOUNT_THRESHOLD)
        .select("transaction_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    out = batch_df.withColumn(
        "is_fraud",
        F.when(
            (F.col("amount") > FRAUD_AMOUNT_THRESHOLD) |
            F.col("transaction_id").isin(fraud_ids),
            True,
        ).otherwise(False)
    ).withColumn(
        "fraud_reason",
        F.when(
            F.col("amount") > FRAUD_AMOUNT_THRESHOLD,
            F.lit(f"HIGH_VALUE > ${FRAUD_AMOUNT_THRESHOLD:.0f}"),
        ).otherwise(F.lit(None).cast(StringType()))
    ).select(
        F.col("user_id"),
        F.to_timestamp(F.col("timestamp")).alias("event_timestamp"),
        F.col("merchant_category"),
        F.col("amount"),
        F.col("location_country"),
        F.col("location_city"),
        F.col("is_fraud"),
        F.col("fraud_reason"),
    )

    write_to_postgres(out, batch_id, "transactions")


# ── Main entry point ──────────────────────────────────────────
def main():
    spark  = build_spark_session()
    stream = read_kafka_stream(spark)

    # ── Stream 1: write ALL transactions ──────────────────────
    all_txn_query = (
        stream.writeStream
        .foreachBatch(write_all_transactions)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/all_transactions")
        .trigger(processingTime="15 seconds")
        .start()
    )
    log.info("▶️   Stream 1 running — all transactions → [transactions]")

    # ── Stream 2: high-value fraud alerts ─────────────────────
    hv_fraud = detect_high_value(stream).select(
        F.col("user_id"),
        F.to_timestamp(F.col("timestamp")).alias("event_timestamp"),
        F.col("merchant_category"),
        F.col("amount"),
        F.col("location_country"),
        F.col("location_city"),
        F.col("fraud_reason"),
    )

    hv_query = (
        hv_fraud.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "fraud_alerts"))
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/high_value_fraud")
        .trigger(processingTime="15 seconds")
        .start()
    )
    log.info("▶️   Stream 2 running — high-value alerts → [fraud_alerts]")

    # ── Stream 3: impossible travel alerts ────────────────────
    travel_fraud = detect_impossible_travel(stream).select(
        F.col("user_id"),
        F.to_timestamp(F.col("timestamp")).alias("event_timestamp"),
        F.col("merchant_category"),
        F.col("amount"),
        F.col("location_country"),
        F.col("location_city"),
        F.col("fraud_reason"),
    )

    travel_query = (
        travel_fraud.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "fraud_alerts"))
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/impossible_travel")
        .trigger(processingTime=f"{TRAVEL_WINDOW_MINUTES * 60} seconds")
        .start()
    )
    log.info("▶️   Stream 3 running — impossible-travel alerts → [fraud_alerts]")

    log.info("=" * 60)
    log.info("  All streaming queries active. Awaiting termination …")
    log.info("=" * 60)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
