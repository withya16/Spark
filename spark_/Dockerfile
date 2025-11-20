FROM python:3.11-slim

# Java (Spark가 내부에서 필요)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 스파크 스트리밍 코드 복사
COPY user_activity_streaming.py .

# 컨테이너가 뜨면 바로 스트리밍 시작
CMD ["python", "user_activity_streaming.py"]