# **1. 전체 실행 순서 요약**

**MinIO 실행 → 버킷 생성 → Kafka/ZooKeeper/Log Generator 실행 → Raw Layer(Spark) 실행 → Silver Layer(Spark) 실행**

> 순서가 꼬이면
> 
> - Spark가 MinIO에 접근 못함
> - checkpoint 꼬여서 offset 리셋됨
> - 메모리 급격한 증가
> - 로컬에서 파일 깨져 보이는 문제 발생
> 
> 그래서 **아래 순서대로 진행할 것**
> 

---

# **2. MinIO 먼저 실행**

### 1) Docker Desktop 켜기

### 2) 프로젝트 폴더에서 실행:

```bash
docker compose up -d minio

```

### 3) MinIO 접속

브라우저 → [http://localhost:9001](http://localhost:9001/)

### 4) 로그인 정보
    
우리 compose 파일에서 에서 설정된 기본값 그대로)
    

### 5) 버킷 생성

반드시 아래 이름 그대로 생성

```
raw-data
silver
# 버킷을 2개 각각 생성해야함. 위에 캡쳐본 보면 뭔 말인지 알 수 있음
```

> 이유: Spark 코드에서 s3a://raw-data, s3a://silver 라고 버킷이름을 하드코딩해놓음 그래서 이름 다르면 바로 에러 터짐
> 

---

# **3. 나머지 컨테이너 실행**

Kafka, ZooKeeper, WAS(로그 생성기)까지 하나씩 올리는 걸 추천

```bash
docker compose up -d zookeeper kafka was

```

확인

```bash
docker ps

```

---

# **4. Raw Layer (Bronze) 먼저 실행해야 함**

### 이유

- Raw Layer가 일정량 데이터를 미리 쌓아야
- Silver Layer가 읽으면서 안정적으로 E-T 가 수행됨
- Silver만 먼저 실행하면 MinIO에 파일이 없어서 오류 또는 메모리 폭증 발생

### 실행

```bash
docker compose up -d 브론즈 컨테이너 이름
```

> Raw Layer는 쉴 새 없이 Kafka → MinIO로 JSON 라인 저장
> 
> 
> 약 **10~20초 정도 데이터 쌓일 때까지 기다리는 걸 추천**
> 

리프레시 눌렀을 때 MinIO에서 `raw-data` 버킷에 파일 생기기 시작하면 굳

---

# **5. Silver Layer 실행**

이제 Silver를 실행

```bash
docker compose up -d 실버 컨테이너 이름

```

### Silver 실행 후 기대되는 결과

MinIO → `silver/user-activity/year=YYYY/...`

아래처럼 파케 폴더 기반 파일 구조가 생성됨:

```
silver/
 └── user-activity/
      └── year=2025/
           └── month=12/
                └── day=12/
                     └── hour=03/
                          ├── part-00000-...
                          ├── part-00001-...

```

---

# **6. 그외**

### 1) 로컬에서 생성된 폴더의 part.1 파일은 깨져 보임 → 정상임

로컬 VSC에서 보이는 파케 조각 파일은 바이너리라 깨져 보이는 게 정상

MinIO에서 **다운로드한 parquet는 정상적으로 열림**

---

### 2) checkpoint 꼬이면 반드시 삭제해야 함

아래 폴더 삭제 후 다시 실행

```
/checkpoints/silver_user_activity
/checkpoints/raw_user_activity
```

> 이유: Kafka offset, stateful 정보 꼬이면 Spark가 예전 offset부터 재처리해서 메모리 폭발함.
근데 일단 코드에서 각주 처리해놔서… 체크포인트 문제로 골머리 앓진 않을 거야.. 근데 암튼 깔끔하게 하고 싶다하면 걍 지워
> 

---

### 3) Silver는 Raw 없이 단독 실행하면 안 된다

Silver는 Raw Layer 출력 파일을 읽고 변환하는 구조라,

- Raw 데이터가 없으면 readStream Pending
- 태초부터 offset 다시 읽어서 메모리 올라감
- Null row 발생
- parquet 쓰기 실패

무조건 **Raw(Bronze) → Silver 순서로**

---

### 4) MinIO는 반드시 코드보다 먼저 실행되어 있어야 한다

Spark가 시작할 때 MinIO endpoint에 연결해서 bucket 체크함.

MinIO가 안 켜져 있으면 아래 에러 발생:

```
S3AFileSystem: IOException: Connection refused

```

---

# **7. 전체 명령어 정리**

```bash
# 0. Docker Desktop 실행

# 1. MinIO 먼저 실행
docker compose up -d minio

# 2. MinIO 로그인 + raw-data, silver 버킷 생성

# 3. 나머지 컨테이너 실행
docker compose up -d zookeeper kafka was

# 4. Raw Layer 먼저 가동
docker compose up -d spark-raw

# (10~20초 대기 후 MinIO raw-data 파일 생성 확인하고 브론즈 컨테이너만 꺼도 됨. 왜냐면 동시에 돌리니까 내 컴퓨터는 자꾸 뻑가서 난 브론즈 컨테이너에서 충분히 데이터 쌓아졌다 생각하면 멈추고 실버 돌아가게 했었음)

# 5. Silver Layer 실행
docker compose up -d spark-silver
```

---

# **8. 종료 명령어**

```bash
docker compose down

```

checkpoint만 남기고 싶으면

```bash
docker compose down --volumes

```
