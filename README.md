## ETL Pipeline 개요

- **Bronze → Silver**: Kafka 로그를 MinIO(S3)로 적재 후 정제  
  담당자: [김주연 (`yoni0319`)](https://github.com/yoni0319)
- **Gold → ClickHouse → Grafana**: Silver Parquet을 집계해 ClickHouse에 적재하고 대시보드 제공  
  담당자: [위지우 (`withya16`)](https://github.com/withya16)

## 주요 구성
- `minio_e.py`: Kafka → Bronze(JSON, MinIO `raw-data/user-activity`)
- `minio_t.py`: Bronze → Silver(Parquet, MinIO `silver/user-activity-v2`)
- `clickhouse_gold_connector.py`: Silver → ClickHouse 집계 테이블 적재
- `docker-compose.yaml`: ZK/Kafka, MinIO, ClickHouse, Grafana, Spark(Bronze/Silver/Gold) 서비스 정의

## 참고
- ClickHouse 스키마: `clickhouse/schema.sql`
- Grafana 대시보드: `grafana/dashboards/monitoring.json`
