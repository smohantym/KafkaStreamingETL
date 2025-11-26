# Kafka → Spark Streaming → Parquet → Airflow Batch ETL (Dockerized)

This project is a small but **end-to-end data engineering pipeline**:

1. A **Python producer** sends synthetic events to **Kafka**.
2. **Spark Structured Streaming** reads from Kafka, cleans + enriches + aggregates the data, and writes **Parquet** files.
3. **Airflow** runs a **daily batch job** (pandas + pyarrow) on Spark’s Parquet output to compute daily revenue metrics.

Everything runs locally via **Docker Compose** and is tuned to demonstrate real data-engineering concepts like joins, windowing, watermarking, AQE, partitioning, and Airflow orchestration.

---

## 1. Architecture

```text
+----------------+        +---------------------------+
|  producer      |        |  airflow                  |
|  (Python)      |        |  - kafka_spark_etl DAG   |
+--------+-------+        |  - daily batch (pandas)  |
         |                +------------+-------------+
         v                             ^
+--------+-----------------------------|-------------+
|          Kafka (3 partitions)        |             |
|  zookeeper + kafka + kafka-init      |             |
+--------+-----------------------------+             |
         |                                           |
         v                                           |
+--------+-------------------------------------------+-------+
|                   Spark cluster                           |
|  spark-master + spark-worker + spark-streaming            |
|                                                           |
|  - spark-streaming runs app.py:                           |
|    * Kafka source (Structured Streaming)                  |
|    * cleaning, UDFs, joins with dim tables               |
|    * watermark + sliding window aggregations             |
|    * writes Parquet partitioned by country               |
+----------------------------------------------------------+
         |
         v
+--------------------------+
|  data/output/parquet     |
|  (shared volume)         |
+--------------------------+
````

Shared volumes:

* `./spark` → `/opt/spark-app` (Spark code)
* `./data/output` → `/opt/spark-output` (Spark) and `/opt/airflow/data` (Airflow)

---

## 2. Tech Stack

* **Kafka**: Confluent cp-kafka / cp-zookeeper images
* **Spark**: 3.5.1 (cluster mode: master + worker + dedicated streaming driver)
* **Airflow**: 2.9.3 (Python 3.11 image)
* **Python**:

  * `pyspark` (inside Spark containers)
  * `kafka-python` for producer
  * `pandas`, `pyarrow` for batch analytics in Airflow
* **Docker Compose**: orchestrates all containers

---

## 3. Services (from `docker-compose.yml`)

### 3.1 Zookeeper

* Image: `confluentinc/cp-zookeeper:7.9.3.arm64`
* Port: `2181`
* Healthcheck: `zookeeper-shell` listing `/` to confirm readiness.

### 3.2 Kafka

* Image: `confluentinc/cp-kafka:7.9.3.arm64`
* Depends on: **healthy** Zookeeper
* Ports:

  * `9092` → internal (for other containers)
  * `9094` → external (for host tools)
* Key env vars:

  * `KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181`
  * `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9094`
* Healthcheck: `kafka-topics --list` against `localhost:9092`.

### 3.3 kafka-init

* Image: same as Kafka (cp-kafka)
* Depends on: **healthy** Kafka
* Behavior:

  * Waits until Kafka is responsive.
  * Creates topic `${KAFKA_TOPIC:-events}` with:

    * 3 partitions
    * replication factor 1
  * Logs existing topics.

### 3.4 Spark master

* Image: `spark:3.5.1-python3`
* Command: `org.apache.spark.deploy.master.Master --host spark-master`
* Ports:

  * `7077` → Spark master endpoint
  * `8080` → Web UI
* Volumes:

  * `./spark` → `/opt/spark-app`
  * `./data/output` → `/opt/spark-output`

### 3.5 Spark worker

* Image: `spark:3.5.1-python3`
* Command: `org.apache.spark.deploy.worker.Worker spark://spark-master:7077`
* Env:

  * `SPARK_WORKER_CORES=2`
  * `SPARK_WORKER_MEMORY=2g`
* Ports:

  * `8081` → Worker UI
* Volumes: same as master (app + output).

### 3.6 Spark streaming driver

* Image: `spark:3.5.1-python3`
* Depends on:

  * Kafka (healthy)
  * kafka-init (completed)
  * spark-master (started)
* Volumes:

  * `./spark` → `/opt/spark-app`
  * `./data/output` → `/opt/spark-output`
  * `./cache/ivy` → `/tmp/.ivy2` (for Ivy cache)
* Entrypoint:

  * Sets `RUN_ID` (timestamp)
  * Clears old checkpoint: `/opt/spark-output/parquet/_chk`
  * Ensures output dir exists and is writable
  * Runs `spark-submit` against `/opt/spark-app/app.py` with:

    * `--master spark://spark-master:7077`
    * `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`
    * `--conf spark.sql.shuffle.partitions=4`
* Env:

  * `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`
  * `KAFKA_TOPIC=${KAFKA_TOPIC:-events}`
  * `OUTPUT_PATH=/opt/spark-output/parquet`

### 3.7 Producer

* Build from `./producer`
* Env:

  * `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`
  * `KAFKA_TOPIC=${KAFKA_TOPIC:-events}`
  * `MSGS_PER_SEC=${MSGS_PER_SEC:-5}`
* Command: `python -u producer.py` (unbuffered logs).

### 3.8 Airflow

* Build from `./airflow`
* Depends on: **spark-streaming** (so streaming is running before batch)
* Env:

  * `AIRFLOW__CORE__EXECUTOR=SequentialExecutor`
  * `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db`
  * `AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Kolkata`
  * `AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags`
* Ports:

  * `8082` → Airflow Web UI
* Volumes:

  * `./airflow/dags` → `/opt/airflow/dags`
  * `./data/output` → `/opt/airflow/data`
* Command:

  * `airflow db init`
  * Ensure `default_pool`
  * Create admin user (`admin` / `admin`)
  * Start webserver + scheduler.

---

## 4. Streaming App — `spark/app.py`

### 4.1 SparkSession & optimizations

```python
spark = (
    SparkSession.builder.appName(f"KafkaStreamingETL_{RUN_ID}")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
    .getOrCreate()
)
```

Demonstrates:

* **Resource tuning**: shuffle partitions, default parallelism.
* **Adaptive Query Execution (AQE)**:

  * Enabled + partition coalescing + skew join handling.
* **Dynamic Partition Pruning (DPP)**.
* **Advanced joins**: disabling automatic broadcast to force merge joins where needed.
* **Streaming checkpointing** for fault tolerance.

### 4.2 Kafka → DataFrame (Structured Streaming)

* Schema for the JSON payload:

```python
event_schema = T.StructType([...])
```

* Streaming source:

```python
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
```

### 4.3 Cleaning, UDFs, dedup

```python
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
    .dropDuplicates(["event_id"])
)
```

Covers:

* **UDFs** (`normalize_device`) + SQL functions (`upper`, `to_timestamp`, `na.fill`).
* **Data quality**:

  * filling missing values
  * dropping duplicate events.

### 4.4 Dimensions, caching & joins

```python
users_data = [...]
users_df = spark.createDataFrame(users_data, schema=users_schema)

products_data = [...]
products_df = spark.createDataFrame(products_data, schema=products_schema)

users_df = users_df.repartition(4, "user_id").cache()
products_df = products_df.repartition(4, "product_id").cache()

users_df_no_country = users_df.drop("country")

enriched = (
    cleaned.join(F.broadcast(users_df_no_country), "user_id", "left")
    .join(products_df, "product_id", "left")
)
```

Concepts:

* **Static dimension tables** in Spark.
* **Repartition + cache**: partitions & persistence on dim tables.
* **Broadcast join**: `F.broadcast(users_df_no_country)` for small users dim.
* **Standard join** for products (shows both styles).

### 4.5 SQL projection

```python
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
```

* Demonstrates using **Spark SQL** on DataFrames.

### 4.6 Watermark, aggregation & windowing

```python
with_watermark = sql_enriched.withWatermark("event_time", "30 seconds")

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
```

* **Watermark** on `event_time` to allow late data.
* **Sliding window**: 1-minute window, 30-second slide.
* **Aggregations**: sum, approx distinct count, max.

### 4.7 Flattening & partitioned output

```python
windowed_flat = (
    windowed
    .withColumn("event_date", F.to_date(F.col("window.end")))
    .drop("window")
)

final_output = windowed_flat.repartition("country")
```

* Adds `event_date` column (for batch processing).
* Drops the nested struct column `window`.
* Repartitions by `country` (helps DPP and downstream reads).

### 4.8 Sinks: Parquet + console

```python
parquet_query = (
    final_output.writeStream.outputMode("append")
    .format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("country")
    .trigger(processingTime="30 seconds")
    .start()
)

console_query = (
    final_output.writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)
```

* **Parquet sink**:

  * File sink in **append** mode, partitioned by `country`.
  * Checkpointing for exactly-once semantics.
* **Console sink**:

  * For debugging – prints updated aggregates each micro-batch.

---

## 5. Producer — `producer/producer.py`

* Uses `kafka-python` to send JSON messages to Kafka.
* Key behavior:

  * Configurable throughput via `MSGS_PER_SEC`.
  * Random `user_id`, `product_id`, `amount`, `country`, `device`.
  * Injects **nulls** and **duplicate event_ids** to exercise cleaning/dedup in Spark.

---

## 6. Airflow DAG — `airflow/dags/kafka_spark_etl.py`

This DAG orchestrates a **daily batch** over the streaming output.

### Tasks

1. **`prepare_params`**

   * Reads Airflow logical date (`ds`).
   * Assembles a dict:

     * `output_path` (default `/opt/airflow/data/parquet`)
     * `run_date` (e.g. `2025-11-26`)
   * Pushes via XCom under key `batch_params`.

2. **`run_batch` / `run_batch_task`**

   * Pulls `batch_params` from XCom.
   * Calls `run_batch_job(output_path, run_date)`:

     * Reads all Parquet files written by Spark streaming.
     * Filters rows for `event_date == run_date`.
     * Aggregates `daily_revenue` & `daily_events` per `(event_date, country, segment)`.
     * Ranks segments within each country by revenue.
     * Builds in-memory `dim_users` and joins on `segment` to add `is_premium`.
     * Aggregates again to `total_revenue` per `(event_date, country, is_premium)`.
     * Writes result into `/opt/airflow/data/daily_segment_metrics/metrics_<run_date>.parquet`.
   * Returns and pushes `row_count` via XCom.

3. **`quality_check`**

   * Pulls `row_count` from XCom.
   * Logs row count and prints a warning if `row_count == 0` (no hard fail in dev).

### DAG definition

* `dag_id="kafka_spark_etl"`
* `schedule_interval="@daily"`
* No catchup (`catchup=False`)
* Task dependencies:

```python
t_prepare >> t_batch >> t_quality
```

---

## 7. How to Run

### 7.1 Prerequisites

* Docker & Docker Compose installed.
* Ports available:

  * `9092`, `9094` (Kafka)
  * `7077`, `8080`, `8081` (Spark)
  * `8082` (Airflow UI)

### 7.2 Start the whole stack

From project root:

```bash
docker compose up -d --build
```

This will:

* Start Zookeeper & Kafka
* Create Kafka topic `events`
* Start Spark master / worker / streaming driver
* Build and start producer
* Build and start Airflow

Check containers:

```bash
docker ps
```

### 7.3 Verify streaming pipeline

1. **Producer logs**:

```bash
docker logs -f producer
```

You should see messages like:

```text
[PRODUCER] Sending messages to events on kafka:9092
[PRODUCER] Sent 50 messages...
```

2. **Spark streaming logs**:

```bash
docker logs -f spark-streaming
```

You should see:

* Query started
* Micro-batch progress
* Console sink output with aggregated rows.

3. **Parquet files on host**:

```bash
ls -R data/output/parquet
```

You should see subdirectories like:

```text
country=IN/
country=US/
country=DE/
```

Each containing Parquet files.

### 7.4 Use Airflow for batch analytics

1. Open Airflow UI:
   [http://localhost:8082](http://localhost:8082)
   Login: `admin` / `admin` (as created in the entrypoint).

2. Enable the DAG `kafka_spark_etl`.

3. Trigger a DAG run via the UI.

4. After it runs:

* Check `run_batch` logs to see `[BATCH]` messages.
* Check `quality_check` logs to see row counts.

5. Verify summary output:

```bash
ls -R data/output/daily_segment_metrics
```

You should see Parquet files like:

```text
metrics_2025-11-26.parquet
```

---

## 8. Mapping to Data Engineering Concepts

This project demonstrates:

1. **Kafka ingestion** with topic partitioning.
2. **Spark Structured Streaming**:

   * Kafka source, JSON parsing.
3. **Data cleaning**:

   * Normalizing device, uppercasing country.
   * Filling missing values.
   * Dropping duplicates.
4. **Joins**:

   * Fact ↔ user dim (broadcast).
   * Fact ↔ product dim (non-broadcast).
5. **Aggregations & groupBy**:

   * Sliding window aggregations per country/segment.
6. **Window functions**:

   * Time windowing (streaming).
   * Ranking segments (batch via pandas; easily portable to Spark batch).
7. **Watermarking & late data handling**.
8. **Partitions & shuffling**:

   * Repartitioning dims.
   * Partitioned Parquet output by country.
9. **Caching & persistence** on dimension tables.
10. **Checkpointing & fault tolerance** for streaming.
11. **Job optimization**:

    * AQE, dynamic partition pruning, skew join handling.
12. **Parameter passing & XComs in Airflow**:

    * `prepare_params` → `run_batch` → `quality_check`.
13. **Multi-step ETL orchestration** via Airflow DAG.