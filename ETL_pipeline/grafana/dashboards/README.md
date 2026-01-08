# Grafana 대시보드

이 폴더에 Grafana 대시보드 JSON 파일을 넣으면 자동으로 로드됩니다.

## 대시보드 예시 쿼리

### 1. 운영/성능 지표

#### 서버 건강도 (1분 단위)
```sql
SELECT 
    minute as time,
    total_requests,
    error_requests,
    error_rate,
    unique_users,
    avg_delay_seconds
FROM metrics_server_health_minutely
WHERE $__timeFilter(minute)
ORDER BY minute DESC
```

#### Endpoint별 에러율
```sql
SELECT 
    path_canon,
    method,
    sum(request_count) as total_requests,
    sum(error_count) as total_errors,
    avg(error_rate) as avg_error_rate
FROM metrics_operational_hourly
WHERE $__timeFilter(hour)
GROUP BY path_canon, method
ORDER BY total_errors DESC
LIMIT 20
```

### 2. 비즈니스 지표

#### 사용자 행동 (일별)
```sql
SELECT 
    date as time,
    event_type,
    action_type,
    sum(event_count) as total_events,
    sum(unique_users) as total_users
FROM metrics_business_daily
WHERE $__timeFilter(date)
GROUP BY date, event_type, action_type
ORDER BY date DESC, total_events DESC
```

#### 퍼널 분석
```sql
-- Step 1: 메인 페이지
SELECT count(DISTINCT user_id) as users
FROM raw_logs
WHERE $__timeFilter(event_time)
  AND action_type = 'view_main'

-- Step 2: 장바구니 추가
SELECT count(DISTINCT user_id) as users
FROM raw_logs
WHERE $__timeFilter(event_time)
  AND action_type = 'add_to_cart'

-- Step 3: 체크아웃
SELECT count(DISTINCT user_id) as users
FROM raw_logs
WHERE $__timeFilter(event_time)
  AND action_type = 'checkout'

-- Step 4: 구매 완료
SELECT count(DISTINCT user_id) as users
FROM raw_logs
WHERE $__timeFilter(event_time)
  AND action_type = 'purchase'
```

### 3. 데이터 품질 지표

#### 데이터 품질 개요
```sql
SELECT 
    hour as time,
    total_records,
    duplicate_count,
    duplicate_rate,
    records_with_null,
    null_rate,
    avg_delay_seconds,
    max_delay_seconds,
    records_missing_user_id
FROM metrics_data_quality_hourly
WHERE $__timeFilter(hour)
ORDER BY hour DESC
```

#### 중복률 추이
```sql
SELECT 
    hour as time,
    duplicate_rate * 100 as duplicate_rate_percent
FROM metrics_data_quality_hourly
WHERE $__timeFilter(hour)
ORDER BY hour DESC
```

#### 시간 지연 추이
```sql
SELECT 
    hour as time,
    avg_delay_seconds,
    max_delay_seconds,
    p95_delay_seconds
FROM metrics_data_quality_hourly
WHERE $__timeFilter(hour)
ORDER BY hour DESC
```


