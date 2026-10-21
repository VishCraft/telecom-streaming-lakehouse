FROM python:3.11-slim

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY scripts ./scripts

CMD ["spark-submit", "--master", "local[2]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,io.delta:delta-spark_4.2_2.13:4.4.1", "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", "app/streaming_pipeline.py"]
