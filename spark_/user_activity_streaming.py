import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

def main():
    # ✅ 환경변수에서 Kafka 주소 읽기 (없으면 localhost:9092)
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    spark = (
        SparkSession.builder
        .appName("UserActivityStreaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # 1) Kafka 스트림 읽기
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "user-activity-logs")
        .option("startingOffsets", "latest")
        .load()
    )

    # 2) JSON 스키마
    log_schema = StructType([
        StructField("eventType", StringType()),
        StructField("endpoint", StringType()),
        StructField("actionType", StringType()),
        StructField("userId", LongType()),
        StructField("message", StringType()),
        StructField("timestamp", StringType()),
        StructField("metadata", StringType()),
    ])

    json_str_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str")

    parsed_df = (
        json_str_df
        .select(from_json(col("json_str"), log_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    query = (
        parsed_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()