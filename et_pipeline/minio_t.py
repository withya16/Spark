import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, substring,
    to_timestamp, year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)
from pyspark.sql.functions import udf


# 1) UDF (Spark SQL로 대체 어려운 항목들)

def extract_action_type(log):
    tokens = re.split(r"\] ?", log)
    if len(tokens) < 2:
        return None
    seg = tokens[1].strip().split(" ")
    if seg[0] == "-":
        return seg[1]
    return seg[0]


def extract_endpoint(log):
    match = re.search(r"(GET|POST|PUT|DELETE)\s+(\S+)", log)
    return match.group(2) if match else None


extract_action_type_udf = udf(extract_action_type, StringType())
extract_endpoint_udf = udf(extract_endpoint, StringType())


# 2) StructType으로 RAW 스키마 선언

raw_schema = StructType([
    StructField("raw_json", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", LongType(), True),
    StructField("offset", LongType(), True),
    StructField("kafka_timestamp", StringType(), True)
])


# 3) 메인 코드

def main():
    spark = (
        SparkSession.builder
        .appName("SilverLayer_UserActivity_Ultimate")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        # Docker가 과하게 안 터지게 Spark 메모리 제한
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        # MinIO(S3A) 설정
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minio"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minio123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    silver_checkpoint = "/app/checkpoints/silver_user_activity"

    # 4) Bronze RAW 읽기 (schema 명시 필수)
    raw_df = (
        spark.readStream
        .format("json")
        .schema(raw_schema)
        # 한 번에 너무 많이 안 읽게 해서 Docker 부담 줄이기
        .option("maxFilesPerTrigger", 5)
        .load("s3a://raw-data/user-activity/")
    )

    # 5) Raw 클렌징 — 로그 앞부분의 깨진 문자열 제거
    cleaned_df = raw_df.withColumn(
        "raw_json_clean",
        regexp_extract(col("raw_json"), r"^.*?(\[[A-Z_]+.*)", 1)
    )

    # 6) Spark SQL 함수로 파싱 가능한 것들
    parsed_df = (
        cleaned_df
        .withColumn("event_type", regexp_extract(col("raw_json_clean"), r"\[(.*?)\]", 1))
        .withColumn("user_id", regexp_extract(col("raw_json_clean"), r"\b(\d{3,6})\b", 1).cast("int"))
        .withColumn(
            "event_time_raw",
            regexp_extract(col("raw_json_clean"), r"(20\d{2}-\d{2}-\d{2}T[\d:\.]+)", 1)
        )
    )

    # 7) UDF 기반 파싱
    parsed_df = (
        parsed_df
        .withColumn("endpoint", extract_endpoint_udf(col("raw_json_clean")))
        .withColumn("action_type", extract_action_type_udf(col("raw_json_clean")))
    )

    # 8) Timestamp 안정화 (나노초 → 마이크로초)
    parsed_df = parsed_df.withColumn(
        "event_time_trim",
        substring(col("event_time_raw"), 1, 26)
    )

    parsed_df = parsed_df.withColumn(
        "event_time",
        to_timestamp(col("event_time_trim"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    )

    # 9) 파티션 컬럼 생성
    clean_df = (
        parsed_df
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
        .withColumn("hour", hour("event_time"))
        .drop("event_time_raw", "event_time_trim", "raw_json_clean")
    )

    # 10) Silver 저장
    silver_query = (
        clean_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", "s3a://silver/user-activity/")
        .option("checkpointLocation", silver_checkpoint)
        .partitionBy("year", "month", "day", "hour")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
