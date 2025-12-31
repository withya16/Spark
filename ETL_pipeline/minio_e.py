import os
from pyspark.sql import SparkSession


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # 실행 제어 플래그
    ENABLE_CONSOLE_LOG = os.getenv("ENABLE_CONSOLE_LOG", "false").lower() == "true"

    # Checkpoint (실험 단위 분리)
    bronze_checkpoint = "/app/checkpoints/bronze_user_activity_v2"
    console_checkpoint = "/app/checkpoints/console_user_activity_v2"

    # SparkSession (관측/디버깅 최적화)
    spark = (
        SparkSession.builder
        .appName("BronzeLayer_UserActivity")
        # 관측 정확성 관련 옵션
        .config("spark.ui.enabled", "true")
        .config("spark.sql.streaming.ui.enabled", "true")
        .config("spark.ui.showConsoleProgress", "true")
        # 패키지
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
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

    # Kafka → Streaming DataFrame
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "user-activity-logs")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Bronze Layer (Raw + Metadata)
    bronze_df = kafka_df.selectExpr(
        "CAST(value AS STRING) AS raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp"
    )

    # Bronze → MinIO
    bronze_query = (
        bronze_df.writeStream
        .format("json")
        .outputMode("append")
        .option("path", "s3a://raw-data/user-activity/")
        .option("checkpointLocation", bronze_checkpoint)
        .start()
    )

    # Console Sink
    if ENABLE_CONSOLE_LOG:
        console_query = (
            bronze_df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("checkpointLocation", console_checkpoint)
            .start()
        )

    # Streaming 유지
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
