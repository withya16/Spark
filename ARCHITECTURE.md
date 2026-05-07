# 프로젝트 아키텍처 & 딥다이브 포인트

## 전체 데이터 흐름

```
[Log Generator]
  ji0513ji/log-generator:1.1.2
  가짜 유저 행동 로그 생성
        │
        ▼ Kafka topic: user-activity-logs
[Kafka (Strimzi / K8s)]
        │
        ▼
[Spark Silver]
  Kafka raw 로그 → 정규표현식 파싱
  → MinIO s3a://silver/user-activity-v3/ (Parquet, year/month/day/hour 파티션)
        │
        ▼
[MinIO]
  silver/  ← Parquet 파일
  gold/    ← checkpoint
        │
        ▼
[Spark Gold]
  Silver Parquet → 5가지 집계
  foreachBatch, 1분 trigger
  → ClickHouse
        │
        ▼
[ClickHouse]
  logs.metrics_server_health_minutely
  logs.metrics_operational_hourly
  logs.metrics_business_daily
  logs.metrics_funnel_daily
  logs.metrics_data_quality_hourly
        │
        ▼
[Grafana]
  ClickHouse datasource 연결
  대시보드 시각화
```

---

## 레이어 구조 (Medallion Architecture)

| 레이어 | 저장소 | 형식 | 역할 |
|---|---|---|---|
| Bronze | Kafka | raw bytes | 원본 로그 수신 |
| Silver | MinIO | Parquet | 정규표현식 파싱, 정제 |
| Gold | ClickHouse | MergeTree | 집계 메트릭, 서빙 |

---

## ClickHouse 테이블 5개

| 테이블 | 집계 단위 | 주요 컬럼 |
|---|---|---|
| metrics_server_health_minutely | 분 | total_requests, error_rate, unique_users |
| metrics_operational_hourly | 시간 | endpoint, method, p50/p95/p99_bytes |
| metrics_business_daily | 일 | event_type, action_type, 이탈 지점 |
| metrics_funnel_daily | 일 | MAIN→CART_ADD→ORDER 전환율 |
| metrics_data_quality_hourly | 시간 | null 비율, avg_delay_seconds |

---

## 이미 구현된 강점

### 1. Window Function으로 이탈 분석
```python
window_spec = Window.partitionBy("date_bucket", "user_id").orderBy(col("event_ts").desc())
dropoff_daily = df.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1)
```
유저별 마지막 이벤트를 추출해서 어디서 이탈했는지 분석.
단순 집계가 아닌 세션 관점의 분석.

---

### 2. 퍼널 분석 (세션 기반)
```python
session_steps = step_df.groupBy("date_bucket", "session_id", "user_id")
    .agg(
        spark_min(when(col("step") == "MAIN", col("event_ts"))).alias("main_ts"),
        ...
    )
```
MAIN → BROWSE → CART_ADD → ORDER 순서를 세션 단위로 추적해서 전환율 계산.
시간 순서가 보장된 퍼널.

---

### 3. Medallion Architecture 실제 구현
Bronze(raw) → Silver(정제) → Gold(집계) 레이어를 별도 Spark job으로 분리해서 운영.
각 레이어가 독립적으로 실행되어 장애 격리 가능.

---

### 4. foreachBatch로 5개 테이블 동시 쓰기
```python
targets = [
    ("metrics_server_health_minutely", ...),
    ("metrics_operational_hourly", ...),
    ...
]
for table_name, out_df in targets:
    out_df.writeTo(f"clickhouse.{CLICKHOUSE_DB}.{table_name}").append()
```
하나의 배치에서 여러 분석 목적의 테이블을 동시에 생성.
배치 재처리 비용을 최소화한 구조.

---

### 5. 정규표현식 기반 로그 파싱
구조화된 JSON이 아닌 raw 로그 문자열을 정규표현식으로 직접 파싱해서 Silver 생성.
실제 운영 로그가 항상 깔끔하지 않다는 걸 경험한 흔적.

---

### 6. p95/p99 응답 분포 측정
```python
expr("percentile_approx(bytes_norm, 0.95)").alias("p95_bytes"),
expr("percentile_approx(bytes_norm, 0.99)").alias("p99_bytes"),
```
평균이 아닌 분위수로 성능 측정.
실제 서비스 모니터링 관점을 반영한 지표 설계.

---

## 앞으로 추가할 강점 포인트

### 1. Watermark (늦은 데이터 처리) — 스트리밍 이해도
현재 Silver/Gold 모두 Watermark가 없어서 늦게 도착하는 데이터가 그냥 섞임.
Watermark를 추가하면 "얼마나 늦은 데이터까지 기다릴 것인가"를 명시적으로 설계한 것으로,
Spark Structured Streaming의 핵심 개념을 이해하고 있다는 걸 보여줄 수 있음.

```python
# 추가 예시
silver_df.withWatermark("event_time", "10 minutes")
```

**왜 중요한가:** 실시간 파이프라인에서 늦은 데이터는 반드시 발생함.
이를 무시하면 집계 결과가 틀릴 수 있음.

---

### 2. Star Schema 설계 — 데이터 모델링
현재 Gold 테이블들은 집계 결과를 flat하게 저장.
Fact/Dimension으로 분리하면 분석 유연성이 올라가고,
데이터 모델링을 제대로 이해하고 있다는 걸 보여줄 수 있음.

```
현재: metrics_operational_hourly (flat)

변경 후:
  dim_endpoint   (endpoint_id, endpoint_canon, http_method)
  dim_date       (date_id, year, month, day, hour)
  fact_requests  (endpoint_id, date_id, request_count, error_count, ...)
```

**왜 중요한가:** 분석 쿼리 성능과 유지보수성이 달라짐.

---

### 3. dbt 도입 — 모던 데이터 스택
현재 Gold 집계 로직이 Spark Python 코드 안에 묻혀 있음.
dbt로 분리하면 SQL로 변환 로직 관리, 테스트 자동화, 데이터 리니지 시각화가 가능해짐.

```
현재: Spark Python → ClickHouse (집계 + 저장 한번에)
변경: Spark → Silver (Parquet) → dbt → Star Schema (ClickHouse)
```

**왜 중요한가:** 팀 협업, 테스트, 문서화가 가능한 구조가 됨.

---

### 4. normalize_path UDF 정교화 — 실제 데이터를 봤다는 증거
현재 정규표현식이 숫자 ID와 UUID만 처리함.

```python
# 현재
path = re.sub(r"/\d+", "/:id", path)
path = re.sub(r"/[0-9a-fA-F-]{8,}", "/:id", path)
```

실제 로그 데이터를 들여다보고 패턴을 더 정교하게 다듬은 흔적을 보여주면
데이터를 실제로 분석했다는 걸 증명할 수 있음.

---

### 5. foreachBatch 에러 핸들링 — 프로덕션 관점
현재 Gold의 write_all_metrics에서 에러 발생 시 그냥 raise만 함.
실패한 배치를 Dead Letter 테이블에 저장하거나 알림을 보내는 구조를 추가하면
프로덕션 운영 관점을 이해하고 있다는 걸 보여줄 수 있음.

---

## 리빌딩 계획

```
1단계: ignoreMissingFiles 수정 (완료)
2단계: Watermark 추가 (Silver/Gold)
3단계: Star Schema 설계
4단계: dbt 도입 (dbt-clickhouse)
5단계: Docker Compose로 로컬 환경 통합
```
