import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, substring,
    to_timestamp, year, month, dayofmonth, hour,
    when, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)
from pyspark.sql.functions import udf

# 1) UDF

def extract_action_type(log):
    if log is None:
        return None
    tokens = re.split(r"\] ?", log)
    if len(tokens) < 2:
        return None
    seg = tokens[1].strip().split(" ")
    if seg[0] == "-":
        return seg[1]
    return seg[0]


def extract_endpoint(log):
    if log is None:
        return None
    match = re.search(r"(GET|POST|PUT|DELETE)\s+(\S+)", log)
    return match.group(2) if match else None


extract_action_type_udf = udf(extract_action_type, StringType())
extract_endpoint_udf = udf(extract_endpoint, StringType())


# 2) Bronze RAW 스키마

raw_schema = StructType([
    StructField("raw_json", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", LongType(), True),
    StructField("offset", LongType(), True),
    StructField("kafka_timestamp", StringType(), True)
])


def main():
    # 실행 제어
    ENABLE_CONSOLE_LOG = os.getenv("ENABLE_CONSOLE_LOG", "false").lower() == "true"

    silver_checkpoint = "/app/checkpoints/silver_user_activity_v2"
    silver_output = "s3a://silver/user-activity-v2/"

    # SparkSession
    spark = (
        SparkSession.builder
        .appName("SilverLayer_UserActivity_v2")

        # Spark UI / Streaming UI
        .config("spark.ui.enabled", "true")
        .config("spark.sql.streaming.ui.enabled", "true")
        .config("spark.ui.showConsoleProgress", "true")

        # 튜닝은 가설 수립 전이므로 주석 유지할께
        # .config("spark.sql.shuffle.partitions", "8")
        # .config("spark.sql.files.maxRecordsPerFile", "200000")

        # Docker 메모리 안전선
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")

        # 패키지
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )

        # MinIO (S3A)
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minio"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minio123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Bronze RAW 읽기
    raw_df = (
        spark.readStream
        .format("json")
        .schema(raw_schema)
        .option("maxFilesPerTrigger", 5)
        .load("s3a://raw-data/user-activity/")
    )

    # raw_json 정리 (Gold 파싱 친화)
    cleaned_df = raw_df.withColumn(
        "raw_json_clean",
        regexp_extract(col("raw_json"), r"^.*?(\[[A-Z_]+.*)", 1)
    )

    # 표준화 컬럼 3종 추가
    standardized_df = (
        cleaned_df
        .withColumn(
            "http_method",
            regexp_extract(col("raw_json_clean"), r"\b(GET|POST|PUT|DELETE)\b", 1)
        )
        .withColumn(
            "status",
            regexp_extract(col("raw_json_clean"), r"\b(API_CALL|API_FINISH|DATA_LOOKUP|FAILED)\b", 1)
        )
        .withColumn(
            "is_failed",
            when(col("status") == lit("FAILED"), lit(1)).otherwise(lit(0))
        )
    )

    # 기존 파싱 유지 (호환성 보존)
    parsed_df = (
        standardized_df
        .withColumn("event_type", regexp_extract(col("raw_json_clean"), r"\[(.*?)\]", 1))
        .withColumn("user_id", regexp_extract(col("raw_json_clean"), r"\b(\d{3,6})\b", 1).cast("int"))
        .withColumn("endpoint", extract_endpoint_udf(col("raw_json_clean")))
        .withColumn("action_type", extract_action_type_udf(col("raw_json_clean")))
        .withColumn(
            "event_time_raw",
            regexp_extract(col("raw_json_clean"), r"(20\d{2}-\d{2}-\d{2}T[\d:\.]+)", 1)
        )
    )

    # Timestamp 안정화
    parsed_df = parsed_df.withColumn("event_time_trim", substring(col("event_time_raw"), 1, 26))
    parsed_df = parsed_df.withColumn(
        "event_time",
        to_timestamp(col("event_time_trim"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    )

    # 파티션 컬럼 + 최종 정리
    final_df = (
        parsed_df
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
        .withColumn("hour", hour("event_time"))
        # raw_json / raw_json_clean 모두 유지
        .drop("event_time_raw", "event_time_trim")
    )

    # 디버깅 모드
    if ENABLE_CONSOLE_LOG:
        (
            final_df.select(
                "event_time",
                "event_type",
                "action_type",
                "http_method",
                "endpoint",
                "status",
                "is_failed",
                "user_id",
                "topic",
                "partition",
                "offset"
            )
            .writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .start()
        )

    # Silver 저장
    (
        final_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", silver_output)
        .option("checkpointLocation", silver_checkpoint)
        .partitionBy("year", "month", "day", "hour")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
