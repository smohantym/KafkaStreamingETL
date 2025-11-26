## 1. Big picture: what this project does

**Data flow:**

1. **Producer** (Python) generates synthetic click/transaction events and sends them to **Kafka**.
2. **Spark Structured Streaming** reads from Kafka, cleans + enriches + aggregates the stream and writes **Parquet** files partitioned by `country`.
3. **Airflow** runs a daily batch job (using **pandas + pyarrow**) that:

   * Reads Spark’s Parquet output
   * Aggregates daily metrics
   * Adds ranking & joins with a tiny dimension
   * Writes daily summary Parquet files.

All of this is orchestrated by **Docker Compose**: Zookeeper + Kafka + Spark master/worker/streaming + producer + Airflow.

---

## 2. `docker-compose.yml` — infrastructure & orchestration

### Zookeeper

```yaml
zookeeper:
  image: confluentinc/cp-zookeeper:7.9.3.arm64
  ...
```

* Runs Zookeeper, required by this Kafka distribution.
* Healthcheck uses `zookeeper-shell` to ensure it’s up before Kafka starts.

### Kafka broker

```yaml
kafka:
  image: confluentinc/cp-kafka:7.9.3.arm64
  depends_on:
    zookeeper:
      condition: service_healthy
  ports:
    - "9092:9092"
    - "9094:9094"
  environment:
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    ...
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9094
```

* Single-broker Kafka cluster.
* Internal listener `kafka:9092` for other containers.
* External listener `localhost:9094` if you want to connect from your host.
* Healthcheck uses `kafka-topics --list`.

### `kafka-init` — one-shot topic setup

```yaml
kafka-init:
  image: confluentinc/cp-kafka:7.9.3.arm64
  depends_on:
    kafka:
      condition: service_healthy
  command: >
    ...
    /usr/bin/kafka-topics --bootstrap-server kafka:9092 \
      --create --if-not-exists \
      --topic "${KAFKA_TOPIC:-events}" \
      --partitions 3 \
      --replication-factor 1;
```

* Runs once at startup.
* Waits until Kafka responds.
* Creates the topic `${KAFKA_TOPIC:-events}` with **3 partitions** (good for parallelism).
* Then exits.

### Spark master

```yaml
spark-master:
  image: spark:3.5.1-python3
  command: ["spark-class","org.apache.spark.deploy.master.Master","--host","spark-master"]
  ports:
    - "7077:7077"   # Spark master
    - "8080:8080"   # Spark master UI
  volumes:
    - ./data/output:/opt/spark-output
    - ./spark:/opt/spark-app
```

* Runs Spark master (`spark://spark-master:7077`).
* Mounts **code** (`./spark`) into `/opt/spark-app`.
* Mounts **output** (`./data/output`) into `/opt/spark-output` (used by streaming job).

### Spark worker

```yaml
spark-worker:
  image: spark:3.5.1-python3
  command: ["spark-class","org.apache.spark.deploy.worker.Worker","spark://spark-master:7077"]
  environment:
    - SPARK_WORKER_CORES=2
    - SPARK_WORKER_MEMORY=2g
  ports:
    - "8081:8081"
  volumes:
    - ./data/output:/opt/spark-output
    - ./spark:/opt/spark-app
```

* Registers with the master, providing 2 cores and 2 GB.
* Mounts same volumes so executors can write/read Parquet.

### Spark streaming driver

```yaml
spark-streaming:
  image: spark:3.5.1-python3
  depends_on:
    kafka: { condition: service_healthy }
    kafka-init: { condition: service_completed_successfully }
    spark-master: { condition: service_started }
  volumes:
    - ./spark:/opt/spark-app
    - ./data/output:/opt/spark-output
    - ./cache/ivy:/tmp/.ivy2
  entrypoint:
    - /bin/bash
    - -lc
    - >
      RUN_ID=$(date +%s);
      export RUN_ID;
      rm -rf /opt/spark-output/parquet/_chk || true;
      mkdir -p /tmp/.ivy2 && mkdir -p /opt/spark-output && chmod -R 777 /opt/spark-output;
      exec /opt/spark/bin/spark-submit
      --master spark://spark-master:7077
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
      --conf spark.sql.shuffle.partitions=4
      --conf spark.jars.ivy=/tmp/.ivy2
      /opt/spark-app/app.py
  environment:
    - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    - KAFKA_TOPIC=${KAFKA_TOPIC:-events}
    - OUTPUT_PATH=/opt/spark-output/parquet
```

* This container runs **`spark-submit app.py`**.
* Uses the Kafka connector package `spark-sql-kafka-0-10`.
* Cleans checkpoint directory on each run, ensures writable output dir.
* Uses env vars so `app.py` is configurable.

### Producer

```yaml
producer:
  build:
    context: ./producer
  depends_on:
    kafka: { condition: service_healthy }
    kafka-init: { condition: service_completed_successfully }
  environment:
    - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    - KAFKA_TOPIC=${KAFKA_TOPIC:-events}
    - MSGS_PER_SEC=${MSGS_PER_SEC:-5}
  command: [ "python","-u","producer.py" ]
```

* Custom image from `producer/Dockerfile`.
* Sends JSON messages to Kafka continuously at configurable rate.

### Airflow

```yaml
airflow:
  build:
    context: ./airflow
  depends_on:
    spark-streaming:
      condition: service_started
  environment:
    AIRFLOW__CORE__EXECUTOR: SequentialExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////opt/airflow/airflow.db
    ...
  ports:
    - "8082:8080"
  volumes:
    - ./airflow/dags:/opt/airflow/dags
    - ./data/output:/opt/airflow/data
  command:
    - bash
    - -lc
    - |
      airflow db init
      airflow pools set default_pool "Default pool" 128 || true
      airflow users create ... || true
      airflow webserver &
      airflow scheduler
```

* Airflow web UI on `localhost:8082`.
* Uses SQLite for metadata (fine for a demo).
* Mounts:

  * DAGs from `./airflow/dags`
  * Streaming output Parquet from `./data/output` → `/opt/airflow/data` so Airflow can read it.
* Starts both webserver and scheduler.

---

## 3. Spark streaming job — `spark/app.py`

This is the heart of the “real-time” side.

### 3.1 Spark session with advanced configs

```python
def build_spark() -> SparkSession:
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

Concepts:

* **Shuffle partitions & parallelism** – tuning for job optimization.
* **AQE (Adaptive Query Execution)**:

  * dynamically coalesces shuffle partitions
  * handles skewed joins
* **Dynamic Partition Pruning (DPP)** – helps later when joining partitioned data.
* **`autoBroadcastJoinThreshold = -1`** – disables size-based auto-broadcast so you can control broadcast manually.
* **Checkpoint location** – required for streaming fault-tolerance.

### 3.2 Read from Kafka as a streaming source

```python
event_schema = T.StructType([...])
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

* Uses **Structured Streaming** with Kafka source.
* Parses JSON messages into columns using a predefined schema.

### 3.3 UDF, cleaning, dedup (data quality)

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

* **UDF**: `normalize_device` cleans device strings.
* **SQL functions**:

  * `upper`, `to_timestamp`, `na.fill`, etc.
* **Data cleaning**:

  * fill missing values (amount, country, device_norm).
  * `dropDuplicates` to remove duplicate events (by `event_id`).

### 3.4 Dimension tables, caching, joins

```python
users_data = [...]
users_schema = ...
users_df = spark.createDataFrame(users_data, schema=users_schema)

products_data = [...]
products_schema = ...
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

* **Static dim tables** created in-memory (no file dependencies).
* **Repartition & cache**:

  * show partitions + shuffling + persistence on static tables.
* **Broadcast join** with users (small dim):

  * manually broadcasting with `F.broadcast`.
* **Normal join** with products:

  * shows both broadcast and non-broadcast joins.

### 3.5 Temp view & SQL projection

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

* Shows using **Spark SQL** on top of a DataFrame.
* “Fact” table now has user segment & premium flag.

### 3.6 Watermark + time window aggregation

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

* **Watermark**:

  * allows late data up to 30 seconds, and decides when a window is final.
* **Windowed aggregation**:

  * 1-min windows sliding every 30 seconds.
* Uses `approx_count_distinct` to be streaming-safe.

### 3.7 Flatten window & partition output

```python
windowed_flat = (
    windowed
    .withColumn("event_date", F.to_date(F.col("window.end")))
    .drop("window")
)

final_output = windowed_flat.repartition("country")
```

* Adds **`event_date`** column derived from window end.
* Drops nested `window` struct → easier for pandas.
* Repartitions by country:

  * aligns with DPP ideas and downstream partitioned reads.

### 3.8 Two sinks: Parquet (append) + console (update)

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

  * append mode (required for file sinks).
  * partitioned by `country`.
  * checkpointing for exactly-once semantics.
* **Console sink**:

  * for debugging – prints aggregates per micro-batch.

---

## 4. Kafka producer — `producer/producer.py`

### Image

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py .
CMD ["python", "producer.py"]
```

* Minimal Python image with `kafka-python`.

### Logic

```python
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "events")
MSGS_PER_SEC = int(os.getenv("MSGS_PER_SEC", "5"))
```

* Controlled via env vars → easy for scaling/testing.

```python
def create_event(i: int) -> dict:
    user_id = random.randint(1, 5)
    product_id = random.randint(1, 8)
    event = {
        "event_id": random_id("evt_"),
        "user_id": user_id,
        "product_id": product_id,
        "amount": round(random.uniform(5, 200), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "country": random.choice(["IN", "US", "US", "US", "DE", None]),
        "device": random.choice(["MOBILE", "mobile ", "DESKTOP", None]),
    }
    ...
```

* Generates synthetic events with:

  * random users/products
  * random amounts
  * real timestamps
  * messy `country` & `device` fields (nulls, extra spaces).

* Introduces:

  * occasional null `amount`
  * occasional **duplicates** (same `event_id`) to exercise dedup in Spark.

The `main()` loop sends events at the target rate, logging every 50 messages.

---

## 5. Airflow image & DAG

### Airflow Dockerfile

```dockerfile
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

* Installs Java (needed if you later want PySpark inside Airflow).
* Installs Python libs: `pandas`, `pyarrow`, `pyspark` (even though DAG currently uses only pandas/pyarrow).

### DAG — `airflow/dags/kafka_spark_etl.py`

**Goal:** Orchestrate a *daily* batch that reads what Spark Streaming wrote and produces daily summary metrics.

#### Constants & imports

```python
DEFAULT_OUTPUT_PATH = "/opt/airflow/data/parquet"
```

* This maps to `./data/output/parquet` via the Docker volume.

#### Task 1: `prepare_params`

```python
def prepare_params(**context):
    ti: TaskInstance = context["ti"]
    run_date = context["ds"]  # 'YYYY-MM-DD'

    params = {
        "output_path": context["params"].get("output_path", DEFAULT_OUTPUT_PATH),
        "run_date": run_date,
    }

    ti.xcom_push(key="batch_params", value=params)
    return params
```

* Reads Airflow’s logical execution date (`ds`).
* Prepares config for the batch job.
* Pushes into **XCom** (both via explicit `xcom_push` and the task return value).

#### Batch function: `run_batch_job`

```python
def run_batch_job(output_path: str, run_date: str) -> int:
    if not os.path.exists(output_path):
        raise FileNotFoundError(...)

    df = pd.read_parquet(output_path)
    ...
```

Steps:

1. **Load** all Parquet files written by Spark streaming.

2. Ensure data exists; if empty, return 0.

3. Convert `event_date` to Date and filter **for the run date**:

   ```python
   df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
   run_date_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
   df_day = df[df["event_date"] == run_date_dt]
   ```

4. **Daily aggregation:**

   ```python
   daily = (
       df_day.groupby(["event_date", "country", "segment"], as_index=False)
       .agg(
           daily_revenue=("total_amount", "sum"),
           daily_events=("unique_events", "sum"),
       )
   )
   ```

5. **Ranking** within each (date, country):

   ```python
   daily["revenue_rank_in_country"] = (
       daily.sort_values([...], ascending=[True, True, False])
       .groupby(["event_date", "country"])
       .cumcount()
       + 1
   )
   ```

6. Tiny **dim_users** in pandas (mirroring Spark dims):

   ```python
   users_data = [...]
   dim_users = pd.DataFrame(users_data, columns=[...])
   dim_seg = dim_users[["segment", "is_premium"]].drop_duplicates("segment")
   joined = daily.merge(dim_seg, on="segment", how="left")
   ```

7. Final **summary**:

   ```python
   summary = (
       joined.groupby(["event_date", "country", "is_premium"], as_index=False)
       .agg(total_revenue=("daily_revenue", "sum"))
   )
   ```

8. Write result to Parquet:

   ```python
   target_dir = os.path.join(os.path.dirname(output_path), "daily_segment_metrics")
   target_path = os.path.join(target_dir, f"metrics_{run_date}.parquet")
   summary.to_parquet(target_path, index=False)
   return len(summary)
   ```

#### Task 2: `run_batch_task`

```python
def run_batch_task(**context):
    ti: TaskInstance = context["ti"]
    params = ti.xcom_pull(task_ids="prepare_params", key="batch_params")

    output_path = params["output_path"]
    run_date = params["run_date"]

    row_count = run_batch_job(output_path=output_path, run_date=run_date)
    ti.xcom_push(key="row_count", value=row_count)
    return row_count
```

* Pulls params via XCom.
* Runs batch job.
* Pushes `row_count` into XCom for the next task.

#### Task 3: `quality_check`

```python
def quality_check(**context):
    ti: TaskInstance = context["ti"]
    row_count = ti.xcom_pull(task_ids="run_batch", key="row_count")
    if row_count is None:
        row_count = ti.xcom_pull(task_ids="run_batch")
    row_count = int(row_count or 0)
    print(f"[QUALITY] Batch row_count = {row_count}")

    if row_count == 0:
        print("[QUALITY] WARNING: No rows produced by batch job (but not failing DAG).")
```

* Reads the row count from XCom.
* Logs a warning instead of failing (dev-friendly).

#### DAG definition & dependencies

```python
with DAG(
    dag_id="kafka_spark_etl",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    ...
) as dag:

    t_prepare = PythonOperator(...)
    t_batch = PythonOperator(...)
    t_quality = PythonOperator(...)

    t_prepare >> t_batch >> t_quality
```

* DAG name: `kafka_spark_etl`.
* Daily schedule; no backfills.
* Classic 3-step ETL pipeline.
