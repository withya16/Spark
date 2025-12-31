FROM eclipse-temurin:11-jre

ENV SPARK_VERSION=3.5.1

WORKDIR /opt

# Install Python
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

# python → python3 심볼릭 링크 생성
RUN ln -s /usr/bin/python3 /usr/bin/python

# Download Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark.tgz && \
    tar -xzf spark.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 spark && \
    rm spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

WORKDIR /app
COPY requirements.txt .
RUN pip3 install --break-system-packages --no-cache-dir -r requirements.txt

COPY . /app

# CMD ["python3", "minio_e.py"]