import os

from pyspark.sql import SparkSession, functions as F, types as T, Window


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/spark-output/parquet")
CHECKPOINT_PATH = os.path.join(OUTPUT_PATH, "_chk")
RUN_ID = os.getenv("RUN_ID", "local_run")


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName(f"KafkaStreamingETL_{RUN_ID}")
        # shuffle / partitions
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        # Adaptive Query Execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Dynamic Partition Pruning
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        # Encourage sort-merge joins for bigger tables by disabling size-based broadcast
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        # Streaming checkpoint dir (needed for fault tolerance)
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = build_spark()

    # --- 1) Kafka source schema ---
    event_schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), True),
            T.StructField("user_id", T.IntegerType(), True),
            T.StructField("product_id", T.IntegerType(), True),
            T.StructField("amount", T.DoubleType(), True),
            T.StructField("event_time", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("device", T.StringType(), True),
        ]
    )

    # --- 2) Read from Kafka as streaming DataFrame ---
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    json_df = kafka_df.select(
        F.from_json(F.col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")

    # --- 3) UDF & data cleaning (point 4 & 19) ---
    @F.udf(returnType=T.StringType())
    def normalize_device(d):
        if d is None:
            return "unknown"
        return d.strip().lower()

    cleaned = (
        json_df.withColumn("device_norm", normalize_device("device"))
        .withColumn("country", F.upper(F.col("country")))
        .withColumn("event_time", F.to_timestamp("event_time"))
        .na.fill({"amount": 0.0, "country": "UNKNOWN", "device_norm": "unknown"})
        # Handle duplicates based on event_id
        # Handle duplicates based on event_dim_path = "/opt/spark-app/resources"id
        .dropDuplicates(["event_id"])
    )

    # Cache cleaned streaming input (metadata only; physical caching used per micro-batch)
    # cleaned = cleaned.persist()

    # --- 4) Build static dimension tables in memory & cache (joins, broadcast, skew) ---

    users_data = [
        (1, "retail", True, "IN"),
        (2, "retail", False, "IN"),
        (3, "enterprise", True, "US"),
        (4, "enterprise", False, "US"),
        (5, "partner", False, "DE"),
    ]

    users_schema = T.StructType(
        [
            T.StructField("user_id", T.IntegerType(), False),
            T.StructField("segment", T.StringType(), True),
            T.StructField("is_premium", T.BooleanType(), True),
            T.StructField("country", T.StringType(), True),
        ]
    )

    users_df = spark.createDataFrame(users_data, schema=users_schema)

    products_data = [
        (1, "books", "low"),
        (2, "electronics", "high"),
        (3, "clothing", "medium"),
        (4, "clothing", "low"),
        (5, "grocery", "low"),
        (6, "grocery", "medium"),
        (7, "services", "high"),
        (8, "services", "medium"),
    ]

    products_schema = T.StructType(
        [
            T.StructField("product_id", T.IntegerType(), False),
            T.StructField("category", T.StringType(), True),
            T.StructField("price_band", T.StringType(), True),
        ]
    )

    products_df = spark.createDataFrame(products_data, schema=products_schema)

    # Repartition & cache dims (partitions & shuffling, caching)
    users_df = users_df.repartition(4, "user_id").cache()
    products_df = products_df.repartition(4, "product_id").cache()

    # IMPORTANT: drop dim.country to avoid duplicate 'country' column after join
    users_df_no_country = users_df.drop("country")

    # Broadcast join for small user dimension (point 6)
    enriched = (
        cleaned.join(F.broadcast(users_df_no_country), "user_id", "left")
        .join(products_df, "product_id", "left")  # normal join to show both types
    )

    enriched.createOrReplaceTempView("fact_events")

    sql_enriched = spark.sql(
        """
        SELECT
          event_id,
          user_id,
          product_id,
          amount,
          event_time,
          country,
          device_norm,
          segment,
          is_premium
        FROM fact_events
        """
    )

    # --- 5) Watermarking & window aggregations (points 2, 3, 10) ---
    # Use small watermark/window so we see output quickly in dev
    with_watermark = sql_enriched.withWatermark("event_time", "30 seconds")

    # 1-minute window sliding every 30 seconds
    windowed = (
        with_watermark.groupBy(
            F.window("event_time", "1 minute", "30 seconds"),
            "country",
            "segment",
        )
        .agg(
            F.sum("amount").alias("total_amount"),
            F.approx_count_distinct("event_id").alias("unique_events"),
            F.max("event_time").alias("max_event_time"),
        )
    )

    # Flatten the window struct into an event_date column so pandas/pyarrow can easily use it
    windowed_flat = (
        windowed
        .withColumn("event_date", F.to_date(F.col("window.end")))
        .drop("window")
    )

    # Partition output by country to enable DPP in later batch job (point 5 & 16)
    final_output = windowed_flat.repartition("country")

    # --- 6) Streaming sinks with checkpointing & fault tolerance ---

    # 6a) Parquet sink (append mode)
    parquet_query = (
        final_output.writeStream.outputMode("append")
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("country")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # 6b) Console sink (for debugging, update mode)
    console_query = (
        final_output.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    print(f"[STREAMING] Writing Parquet to {OUTPUT_PATH} with checkpoint {CHECKPOINT_PATH}")
    parquet_query.awaitTermination()
    console_query.awaitTermination()

if __name__ == "__main__":
    main()
