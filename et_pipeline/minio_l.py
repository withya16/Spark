import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, when, count, countDistinct,
    sum as fsum, expr, to_timestamp, lit
)

def _parse_target_date(target_date: str):
    y, m, d = target_date.split("-")
    return int(y), int(m), int(d)

def main():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access = os.getenv("MINIO_ACCESS_KEY", "minio")
    minio_secret = os.getenv("MINIO_SECRET_KEY", "minio123")

    target_date = os.getenv("TARGET_DATE", "").strip()
    target_hour = os.getenv("TARGET_HOUR", "").strip()

    shuffle_partitions = os.getenv("SHUFFLE_PARTITIONS", "24").strip()
    enable_dup_check = os.getenv("ENABLE_DUP_CHECK", "0").strip()  # 1이면 dup 계산

    spark = (
        SparkSession.builder
        .appName("GoldLayer_UserActivity")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # 성능 튜닝
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # ===== Silver Read =====
    df = spark.read.parquet("s3a://silver/user-activity/")

    # 파티션 컬럼 프루닝
    if target_date:
        y, m, d = _parse_target_date(target_date)
        df = df.filter((col("year") == y) & (col("month") == m) & (col("day") == d))

    if target_hour:
        df = df.filter(col("hour") == int(target_hour))

    # ===== Derived Columns =====
    http_method = regexp_extract(col("raw_json"), r"\b(GET|POST|PUT|DELETE)\b", 1)
    status = regexp_extract(col("raw_json"), r"\b(API_CALL|API_FINISH|DATA_LOOKUP|FAILED)\b", 1)

    df2 = (
        df
        .withColumn("http_method", when(http_method == "", lit(None)).otherwise(http_method))
        .withColumn("status", when(status == "", lit(None)).otherwise(status))
        .withColumn("is_failed", (col("status") == lit("FAILED")).cast("int"))
        # kafka_timestamp가 타임존 포함이면 포맷 지정이 더 안전함(필요시 아래 포맷으로 바꿔)
        .withColumn("kafka_ts", to_timestamp(col("kafka_timestamp")))
    )

    # =========================================
    # (A) fact_hourly_endpoint_health
    # =========================================
    fact = (
        df2.groupBy("year", "month", "day", "hour", "endpoint", "http_method")
           .agg(
               count(lit(1)).alias("event_cnt"),
               fsum(col("is_failed")).alias("fail_cnt"),
               countDistinct(col("user_id")).alias("dau_users"),
           )
           .withColumn("fail_rate", (col("fail_cnt") / col("event_cnt")))
    )

    (fact.write.mode("overwrite")
         .partitionBy("year", "month", "day", "hour")
         .parquet("s3a://gold/fact_hourly_endpoint_health/"))

    # =========================================
    # (B) summary_daily_funnel (1-pass 집계로 가볍게)
    # 퍼널: MAIN(GET /main) -> CART_ADD(POST /cart) -> ORDER(POST /order)
    # =========================================
    is_main = (col("endpoint") == "/main") & (col("http_method") == "GET")
    is_cart_add = (col("endpoint") == "/cart") & (col("http_method") == "POST")
    is_order = (col("endpoint") == "/order") & (col("http_method") == "POST")

    funnel = (
        df2.groupBy("year", "month", "day")
           .agg(
               countDistinct(when(is_main, col("user_id"))).alias("users_main"),
               countDistinct(when(is_cart_add, col("user_id"))).alias("users_cart_add"),
               countDistinct(when(is_order, col("user_id"))).alias("users_order"),
           )
           .na.fill(0, subset=["users_main","users_cart_add","users_order"])
           .withColumn(
               "main_to_cart_add_rate",
               when(col("users_main")==0, lit(0.0)).otherwise(col("users_cart_add")/col("users_main"))
           )
           .withColumn(
               "cart_add_to_order_rate",
               when(col("users_cart_add")==0, lit(0.0)).otherwise(col("users_order")/col("users_cart_add"))
           )
           .withColumn(
               "main_to_order_rate",
               when(col("users_main")==0, lit(0.0)).otherwise(col("users_order")/col("users_main"))
           )
    )

    (funnel.write.mode("overwrite")
           .partitionBy("year","month","day")
           .parquet("s3a://gold/summary_daily_funnel/"))

    # =========================================
    # (C) summary_daily_quality
    # =========================================
    quality_base = (
        df2.groupBy("year", "month", "day")
           .agg(
               count(lit(1)).alias("row_cnt"),
               fsum(when(col("user_id").isNull(), 1).otherwise(0)).alias("null_user_id_cnt"),
               fsum(when(col("endpoint").isNull(), 1).otherwise(0)).alias("null_endpoint_cnt"),
               fsum(when(col("event_time").isNull(), 1).otherwise(0)).alias("null_event_time_cnt"),
           )
    )

    lateness = (
        df2.filter(col("kafka_ts").isNotNull() & col("event_time").isNotNull())
           .withColumn("lateness_sec", (col("kafka_ts").cast("long") - col("event_time").cast("long")))
    )
    lateness_p95 = (
        lateness.groupBy("year", "month", "day")
                .agg(expr("percentile_approx(lateness_sec, 0.95)").alias("lateness_p95_sec"))
    )

    quality = quality_base.join(lateness_p95, ["year","month","day"], "left")

    # dup 체크는 데모에서는 무겁기 때문에 옵션 처리
    if enable_dup_check == "1":
        dup_cnt = (
            df2.groupBy("year", "month", "day", "topic", "partition", "offset")
               .agg(count(lit(1)).alias("c"))
               .filter(col("c") > 1)
               .groupBy("year", "month", "day")
               .agg(count(lit(1)).alias("dup_kafka_offset_cnt"))
        )
        quality = quality.join(dup_cnt, ["year","month","day"], "left").na.fill(0, subset=["dup_kafka_offset_cnt"])

    (quality.write.mode("overwrite")
            .partitionBy("year","month","day")
            .parquet("s3a://gold/summary_daily_quality/"))

    spark.stop()

if __name__ == "__main__":
    main()