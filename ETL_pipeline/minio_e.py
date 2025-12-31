import os
from pyspark.sql import SparkSession

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Bronze 체크포인트
    bronze_checkpoint = "/app/checkpoints/bronze_user_activity"
    console_checkpoint = "/app/checkpoints/console_user_activity"

    # SparkSession (Kafka + MinIO)
    spark = (
        SparkSession.builder
        .appName("BronzeLayer_UserActivity")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minio"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minio123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Kafka → DataFrame
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "user-activity-logs")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Bronze Layer (raw json + metadata)
    bronze_df = kafka_df.selectExpr(
        "CAST(value AS STRING) AS raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp"
    )

    # Bronze → MinIO(json)
    bronze_query = (
        bronze_df.writeStream
        .format("json")
        .outputMode("append")
        .option("path", "s3a://raw-data/user-activity/")
        .option("checkpointLocation", bronze_checkpoint)
        .start()
    )

    # Console 출력도 checkpoint 유지
    console_query = (
        bronze_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", console_checkpoint)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
