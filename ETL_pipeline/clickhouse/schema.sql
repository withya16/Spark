-- 1) 서버 건강도 (분 단위)
CREATE TABLE IF NOT EXISTS logs.metrics_server_health_minutely
(
    minute DateTime,
    total_requests UInt64,
    error_requests UInt64,
    error_rate Float64,
    total_bytes Int64,
    unique_users UInt64,
    unique_sessions UInt64,
    avg_delay_seconds Float64
)
ENGINE = MergeTree()
ORDER BY minute;

-- 2) Endpoint 건강도 (시간 단위)
CREATE TABLE IF NOT EXISTS logs.metrics_operational_hourly
(
    hour DateTime,
    endpoint String,
    method String,
    request_count UInt64,
    total_bytes Int64,
    avg_response_bytes Float64,
    error_count UInt64,
    error_rate Float64,
    unique_users UInt64,
    p50_bytes Float64,
    p95_bytes Float64,
    p99_bytes Float64
)
ENGINE = MergeTree()
ORDER BY (hour, endpoint, method);

-- 3) 사용자 행동/이탈 (일별)
CREATE TABLE IF NOT EXISTS logs.metrics_business_daily
(
    date Date,
    metric_kind String,
    event_type Nullable(String),
    action_type Nullable(String),
    dropoff_event_type Nullable(String),
    dropoff_action_type Nullable(String),
    event_count UInt64,
    unique_users UInt64,
    unique_sessions UInt64
)
ENGINE = MergeTree()
ORDER BY (date, metric_kind);

-- 4) 퍼널 분석 (시간 단위)
CREATE TABLE IF NOT EXISTS logs.metrics_funnel_hourly
(
    hour_bucket DateTime,
    funnel String,
    main_users UInt64,
    cart_add_users UInt64,
    order_users UInt64,
    conv_main_to_order Float64
)
ENGINE = MergeTree()
ORDER BY (hour_bucket, funnel);

-- 5) 데이터 품질 (시간 단위)
CREATE TABLE IF NOT EXISTS logs.metrics_data_quality_hourly
(
    hour DateTime,
    total_records UInt64,
    records_with_null UInt64,
    avg_delay_seconds Float64
)
ENGINE = MergeTree()
ORDER BY hour;
