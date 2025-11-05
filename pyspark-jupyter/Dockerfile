# ============================
# STAGE 1: baixar todos os JARs
# ============================
FROM debian:stable-slim AS jarsdownloader

ARG DELTA_SPARK_SCALA=2.12
ARG DELTA_SPARK_VERSION=3.1.0          # compatível com Spark 3.5.0
ARG DELTA_STORAGE_VERSION=3.1.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.367

# Kafka <-> Spark (3.5.0)
ARG SPARK_KAFKA_VERSION=3.5.0
ARG KAFKA_CLIENTS_VERSION=3.5.0
ARG LZ4_VERSION=1.8.0
ARG SNAPPY_VERSION=1.1.10.5
ARG COMMONS_POOL2_VERSION=2.11.1

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/jars

# ---- Delta Lake (extensions + storage)
RUN curl -fsSL "https://repo1.maven.org/maven2/io/delta/delta-spark_${DELTA_SPARK_SCALA}/${DELTA_SPARK_VERSION}/delta-spark_${DELTA_SPARK_SCALA}-${DELTA_SPARK_VERSION}.jar" \
    -o /opt/jars/delta-spark_${DELTA_SPARK_SCALA}-${DELTA_SPARK_VERSION}.jar
RUN curl -fsSL "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_STORAGE_VERSION}/delta-storage-${DELTA_STORAGE_VERSION}.jar" \
    -o /opt/jars/delta-storage-${DELTA_STORAGE_VERSION}.jar

# ---- S3A (MinIO)
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    -o /opt/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar
RUN curl -fsSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
    -o /opt/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

# ---- Kafka (conector + dependências)
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar" \
    -o /opt/jars/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar" \
    -o /opt/jars/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
    -o /opt/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar
RUN curl -fsSL "https://repo1.maven.org/maven2/org/lz4/lz4-java/${LZ4_VERSION}/lz4-java-${LZ4_VERSION}.jar" \
    -o /opt/jars/lz4-java-${LZ4_VERSION}.jar
RUN curl -fsSL "https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/${SNAPPY_VERSION}/snappy-java-${SNAPPY_VERSION}.jar" \
    -o /opt/jars/snappy-java-${SNAPPY_VERSION}.jar
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
    -o /opt/jars/commons-pool2-${COMMONS_POOL2_VERSION}.jar


# ============================
# STAGE 2: imagem final pyspark
# ============================
FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# (belt & suspenders) Garante JDK no runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

# Copia todos os JARs pro classpath do Spark
COPY --from=jarsdownloader /opt/jars/*.jar /usr/local/spark/jars/

# spark-defaults.conf — fonte única de verdade (Delta + S3A + Kafka)
# (evita depender de variáveis de ambiente/VS Code e garante bootstrap consistente)
ARG DELTA_SPARK_SCALA=2.12
ARG DELTA_SPARK_VERSION=3.1.0
ARG DELTA_STORAGE_VERSION=3.1.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.367
ARG SPARK_KAFKA_VERSION=3.5.0
ARG KAFKA_CLIENTS_VERSION=3.5.0
ARG LZ4_VERSION=1.8.0
ARG SNAPPY_VERSION=1.1.10.5

RUN mkdir -p /usr/local/spark/conf && \
    printf "%s\n" \
"spark.jars=/usr/local/spark/jars/delta-spark_${DELTA_SPARK_SCALA}-${DELTA_SPARK_VERSION}.jar,/usr/local/spark/jars/delta-storage-${DELTA_STORAGE_VERSION}.jar,/usr/local/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar,/usr/local/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar,/usr/local/spark/jars/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar,/usr/local/spark/jars/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar,/usr/local/spark/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar,/usr/local/spark/jars/lz4-java-${LZ4_VERSION}.jar,/usr/local/spark/jars/snappy-java-${SNAPPY_VERSION}.jar,/usr/local/spark/jars/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
"spark.driver.extraClassPath=/usr/local/spark/jars/*" \
"spark.executor.extraClassPath=/usr/local/spark/jars/*" \
"spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
"spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
> /usr/local/spark/conf/spark-defaults.conf

# libs Python úteis
RUN pip install --no-cache-dir kafka-python boto3 minio pyspark==3.5.0 delta-spark==3.1.0

# volta para o usuário padrão do Jupyter stack
USER ${NB_UID}
