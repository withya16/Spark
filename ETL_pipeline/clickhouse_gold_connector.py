import os
import re

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, current_timestamp,
    count, sum as spark_sum, avg, countDistinct,
    expr, max as spark_max, min as spark_min, row_number
)
from pyspark.sql.streaming import Trigger
from pyspark.sql.types import (
    StringType, StructType, StructField,
    LongType, IntegerType, TimestampType
)

def normalize_path(path: str):
    if path is None:
        return None
    path = path.strip()
    path = re.sub(r"/\d+", "/:id", path)
    path = re.sub(r"/[0-9a-fA-F-]{8,}", "/:id", path)
    path = path.rstrip("/") or "/"
    return path.lower()

def is_df_empty(df) -> bool:
    try:
        return df.isEmpty()
    except Exception:
        return df.rdd.isEmpty()

def main():
    ENABLE_CONSOLE_LOG = os.getenv("ENABLE_CONSOLE_LOG", "false").lower() == "true"

    gold_checkpoint = "/app/checkpoints/gold_clickhouse_spark351_prod_v3"
    silver_input = "s3a://silver/user-activity-v2/"

    # ClickHouse 설정
    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    CLICKHOUSE_PROTOCOL = os.getenv("CLICKHOUSE_PROTOCOL", "http")
    CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", os.getenv("CLICKHOUSE_PORT", "8123"))
    CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "logs")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "backtoeng")

    SPARK_BINARY_VERSION = "3.5"
    SCALA_BINARY_VERSION = os.getenv("SCALA_BINARY_VERSION", "2.12")
    CLICKHOUSE_SPARK_CONNECTOR_VERSION = os.getenv("CLICKHOUSE_SPARK_CONNECTOR_VERSION", "0.9.0")
    CLICKHOUSE_JDBC_VERSION = os.getenv("CLICKHOUSE_JDBC_VERSION", "0.9.4")

    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        f"com.clickhouse.spark:clickhouse-spark-runtime-{SPARK_BINARY_VERSION}_{SCALA_BINARY_VERSION}:{CLICKHOUSE_SPARK_CONNECTOR_VERSION}",
        f"com.clickhouse:clickhouse-jdbc:{CLICKHOUSE_JDBC_VERSION}",
    ]

    spark = (
        SparkSession.builder
        .appName("GoldLayer_ClickHouse_Spark351_Prod_AllMetrics")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        # MinIO(S3A) 설정
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minio"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minio123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # ClickHouse Catalog 설정
        .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        .config("spark.sql.catalog.clickhouse.host", CLICKHOUSE_HOST)
        .config("spark.sql.catalog.clickhouse.protocol", CLICKHOUSE_PROTOCOL)
        .config("spark.sql.catalog.clickhouse.http_port", str(CLICKHOUSE_HTTP_PORT))
        .config("spark.sql.catalog.clickhouse.user", CLICKHOUSE_USER)
        .config("spark.sql.catalog.clickhouse.password", CLICKHOUSE_PASSWORD)
        .config("spark.sql.catalog.clickhouse.database", CLICKHOUSE_DB)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    normalize_path_udf = spark.udf.register("normalize_path", normalize_path, StringType())

    # Schema 정의
    silver_schema = StructType([
        StructField("raw_json", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", LongType(), True),
        StructField("offset", LongType(), True),
        StructField("kafka_timestamp", StringType(), True),
        StructField("raw_json_clean", StringType(), True),
        StructField("http_method", StringType(), True),
        StructField("status", StringType(), True),
        StructField("is_failed", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("endpoint", StringType(), True),
        StructField("action_type", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
    ])

    silver_df = (
        spark.readStream
        .format("parquet")
        .schema(silver_schema)
        .option("maxFilesPerTrigger", 10)
        .load(silver_input)
    )

    # 전처리 로직
    processed_df = (
        silver_df
        .withColumn("http_method", when(col("http_method") == "", lit(None)).otherwise(col("http_method")))
        .withColumn("status", when(col("status") == "", lit(None)).otherwise(col("status")))
        .withColumn("is_failed", when(col("status") == lit("FAILED"), lit(1)).otherwise(lit(0)).cast("int"))
        .withColumn("event_ts", col("event_time"))
        .withColumn("collected_time", current_timestamp())
        .withColumn("endpoint_canon", normalize_path_udf(col("endpoint")))
        .withColumn("session_id", col("user_id").cast(StringType()))
        .withColumn("delay_seconds", col("collected_time").cast("long") - col("event_ts").cast("long"))
        .withColumn("bytes_norm", lit(0).cast("long"))
        .withColumn(
            "null_count",
            (when(col("user_id").isNull(), 1).otherwise(0) +
             when(col("event_type").isNull(), 1).otherwise(0) +
             when(col("endpoint").isNull(), 1).otherwise(0) +
             when(col("event_ts").isNull(), 1).otherwise(0) +
             when(col("http_method").isNull(), 1).otherwise(0))
        )
    )

    def write_all_metrics(batch_df, batch_id: int):
        try:
            if is_df_empty(batch_df):
                return

            df = (
                batch_df
                .filter(col("event_ts").isNotNull())
                .withColumn("minute", expr("date_trunc('minute', event_ts)"))
                .withColumn("hour_bucket", expr("date_trunc('hour', event_ts)"))
                .withColumn("date_bucket", expr("date_trunc('day', event_ts)"))
            )

            # 1) 서버 건강도 (분 단위)
            metrics_server_health_minutely = (
                df.groupBy("minute")
                .agg(
                    count("*").alias("total_requests"),
                    spark_sum(col("is_failed")).alias("error_requests"),
                    (spark_sum(col("is_failed")) / count("*")).alias("error_rate"),
                    spark_sum("bytes_norm").alias("total_bytes"),
                    countDistinct("user_id").alias("unique_users"),
                    countDistinct("session_id").alias("unique_sessions"),
                    avg("delay_seconds").alias("avg_delay_seconds"),
                )
            )

            # 2) Endpoint 건강도 (시간 단위)
            metrics_operational_hourly = (
                df.filter(col("endpoint_canon").isNotNull() & col("http_method").isNotNull())
                .groupBy("hour_bucket", "endpoint_canon", "http_method")
                .agg(
                    count("*").alias("request_count"),
                    spark_sum("bytes_norm").alias("total_bytes"),
                    avg("bytes_norm").alias("avg_response_bytes"),
                    spark_sum(col("is_failed")).alias("error_count"),
                    (spark_sum(col("is_failed")) / count("*")).alias("error_rate"),
                    countDistinct("user_id").alias("unique_users"),
                    expr("percentile_approx(bytes_norm, 0.5)").alias("p50_bytes"),
                    expr("percentile_approx(bytes_norm, 0.95)").alias("p95_bytes"),
                    expr("percentile_approx(bytes_norm, 0.99)").alias("p99_bytes"),
                )
                .select(
                    col("hour_bucket").alias("hour"),
                    col("endpoint_canon").alias("endpoint"),
                    col("http_method").alias("method"),
                    "request_count", "total_bytes", "avg_response_bytes",
                    "error_count", "error_rate", "unique_users",
                    "p50_bytes", "p95_bytes", "p99_bytes",
                )
            )

            # 3) 사용자 행동 및 이탈 (일별)
            action_daily = (
                df.groupBy("date_bucket", "event_type", "action_type")
                .agg(
                    count("*").alias("event_count"),
                    countDistinct("user_id").alias("unique_users"),
                    countDistinct("session_id").alias("unique_sessions"),
                )
                .withColumn("metric_kind", lit("ACTION"))
                .withColumn("dropoff_event_type", lit(None).cast("string"))
                .withColumn("dropoff_action_type", lit(None).cast("string"))
            )

            # 이탈 지점 분석 (사용자별 마지막 이벤트 추출)
            window_spec = Window.partitionBy("date_bucket", "user_id").orderBy(col("event_ts").desc())
            
            dropoff_daily = (
                df.filter(col("user_id").isNotNull())
                .withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
                .groupBy("date_bucket", col("event_type").alias("dropoff_event_type"), col("action_type").alias("dropoff_action_type"))
                .agg(
                    countDistinct("user_id").alias("unique_users"),
                    countDistinct("session_id").alias("unique_sessions"),
                    count("*").alias("event_count"),
                )
                .withColumn("metric_kind", lit("DROPOFF"))
                .withColumn("event_type", lit(None).cast("string"))
                .withColumn("action_type", lit(None).cast("string"))
            )

            metrics_business_daily = (
                action_daily.select(
                    col("date_bucket").alias("date"), "metric_kind",
                    "event_type", "action_type", "dropoff_event_type", "dropoff_action_type",
                    "event_count", "unique_users", "unique_sessions"
                )
                .unionByName(
                    dropoff_daily.select(
                        col("date_bucket").alias("date"), "metric_kind",
                        "event_type", "action_type", "dropoff_event_type", "dropoff_action_type",
                        "event_count", "unique_users", "unique_sessions"
                    ), allowMissingColumns=True
                )
            )

            # 4) 퍼널 (일 단위)
            step_df = (
                df.filter(col("user_id").isNotNull() & col("session_id").isNotNull())
                .withColumn(
                    "step",
                    when((col("endpoint_canon") == "/main") & (col("http_method") == "GET"), lit("MAIN"))
                    .when((col("http_method") == "GET") & ((col("endpoint_canon") == "/search") | (col("endpoint_canon") == "/store/category") | (col("endpoint_canon") == "/store/:id")), lit("BROWSE"))
                    .when((col("endpoint_canon") == "/cart") & (col("http_method") == "POST"), lit("CART_ADD"))
                    .when((col("endpoint_canon") == "/order") & (col("http_method") == "POST"), lit("ORDER"))
                    .when((col("endpoint_canon") == "/orderstatus") & (col("http_method") == "GET"), lit("ORDER_STATUS"))
                    .otherwise(lit(None))
                )
                .filter(col("step").isNotNull())
            )

            session_steps = (
                step_df.groupBy("date_bucket", "session_id", "user_id")
                .agg(
                    spark_min(when(col("step") == "MAIN", col("event_ts"))).alias("main_ts"),
                    spark_min(when(col("step") == "BROWSE", col("event_ts"))).alias("browse_ts"),
                    spark_min(when(col("step") == "CART_ADD", col("event_ts"))).alias("cart_add_ts"),
                    spark_min(when(col("step") == "ORDER", col("event_ts"))).alias("order_ts"),
                    spark_min(when(col("step") == "ORDER_STATUS", col("event_ts"))).alias("order_status_ts"),
                )
            )

            session_steps_alias = session_steps.alias("ss")

            f1_daily = (
                session_steps_alias
                .withColumn("funnel", lit("F1_MAIN_CART_ADD_ORDER"))
                .withColumn("reach_main", col("main_ts").isNotNull())
                .withColumn("reach_cart_add", col("cart_add_ts").isNotNull() & col("main_ts").isNotNull() & (col("cart_add_ts") >= col("main_ts")))
                .withColumn("reach_order", col("order_ts").isNotNull() & col("cart_add_ts").isNotNull() & col("main_ts").isNotNull() & (col("order_ts") >= col("cart_add_ts")))
                .groupBy("date_bucket", "funnel")
                .agg(
                    countDistinct(when(col("reach_main"), col("ss.user_id"))).alias("main_users"),
                    countDistinct(when(col("reach_cart_add"), col("ss.user_id"))).alias("cart_add_users"),
                    countDistinct(when(col("reach_order"), col("ss.user_id"))).alias("order_users"),
                )
                .withColumn("conv_main_to_order", when(col("main_users") > 0, col("order_users") / col("main_users")).otherwise(lit(0.0)))
                .select(
                    col("date_bucket").alias("date"),
                    "funnel",
                    "main_users",
                    "cart_add_users",
                    "order_users",
                    "conv_main_to_order"
                )
            )

            # 5) 데이터 품질
            metrics_data_quality_hourly = (
                df.groupBy("hour_bucket")
                .agg(
                    count("*").alias("total_records"),
                    spark_sum(when(col("null_count") > 0, 1).otherwise(0)).alias("records_with_null"),
                    avg("delay_seconds").alias("avg_delay_seconds"),
                )
                .select(
                    col("hour_bucket").alias("hour"),
                    col("total_records"),
                    col("records_with_null"),
                    col("avg_delay_seconds")
                )
            )

            # ClickHouse 쓰기
            targets = [
                ("metrics_server_health_minutely", metrics_server_health_minutely),
                ("metrics_operational_hourly", metrics_operational_hourly),
                ("metrics_business_daily", metrics_business_daily),
                ("metrics_funnel_daily", f1_daily),
                ("metrics_data_quality_hourly", metrics_data_quality_hourly),
            ]

            for table_name, out_df in targets:
                if not is_df_empty(out_df):
                    out_df.writeTo(f"clickhouse.{CLICKHOUSE_DB}.{table_name}").append()
                    print(f"[batch={batch_id}] Successfully wrote to {table_name}")

        except Exception as e:
            print(f"[batch={batch_id}] Error: {e}")
            raise

    # 스트리밍 시작
    query = (
        processed_df.writeStream
        .foreachBatch(write_all_metrics)
        .option("checkpointLocation", gold_checkpoint)
        .trigger(processingTime="1 minute")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()