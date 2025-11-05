# ====== STAGE 1: baixar JARs ======
FROM debian:stable-slim AS jarsdownloader

ARG DELTA_SPARK_SCALA=2.12
ARG DELTA_SPARK_VERSION=3.1.0          # <-- downgrade
ARG DELTA_STORAGE_VERSION=3.1.0        # <-- downgrade
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.367
ARG SPARK_KAFKA_VERSION=3.5.0

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/jars

# delta-spark (extensions)
RUN curl -fsSL "https://repo1.maven.org/maven2/io/delta/delta-spark_${DELTA_SPARK_SCALA}/${DELTA_SPARK_VERSION}/delta-spark_${DELTA_SPARK_SCALA}-${DELTA_SPARK_VERSION}.jar" \
    -o /opt/jars/delta-spark_${DELTA_SPARK_SCALA}-${DELTA_SPARK_VERSION}.jar

# delta-storage (dependência necessária)
RUN curl -fsSL "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_STORAGE_VERSION}/delta-storage-${DELTA_STORAGE_VERSION}.jar" \
    -o /opt/jars/delta-storage-${DELTA_STORAGE_VERSION}.jar

# hadoop-aws + aws sdk
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    -o /opt/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar
RUN curl -fsSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
    -o /opt/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

# spark-sql-kafka (ok com Spark 3.5.0)
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar" \
    -o /opt/jars/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar

# ====== STAGE 2 ======
FROM jupyter/pyspark-notebook:spark-3.5.0
USER root

# JDK (belt & suspenders)
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-11-jdk && rm -rf /var/lib/apt/lists/*

# Copia jars
COPY --from=jarsdownloader /opt/jars/*.jar /usr/local/spark/jars/

# spark-defaults.conf — fonte única de verdade
RUN mkdir -p /usr/local/spark/conf && \
    echo "spark.jars=/usr/local/spark/jars/delta-spark_${DELTA_SPARK_SCALA}-${DELTA_SPARK_VERSION}.jar,/usr/local/spark/jars/delta-storage-${DELTA_STORAGE_VERSION}.jar,/usr/local/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar,/usr/local/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar,/usr/local/spark/jars/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar" > /usr/local/spark/conf/spark-defaults.conf && \
    echo "spark.driver.extraClassPath=/usr/local/spark/jars/*" >> /usr/local/spark/conf/spark-defaults.conf && \
    echo "spark.executor.extraClassPath=/usr/local/spark/jars/*" >> /usr/local/spark/conf/spark-defaults.conf && \
    echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" >> /usr/local/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> /usr/local/spark/conf/spark-defaults.conf

# (opcional) remova isso para não interferir no classpath base
# ENV SPARK_CLASSPATH=/usr/local/spark/jars/*

# libs Python que você adicionou
RUN pip install --no-cache-dir kafka-python boto3 minio

USER ${NB_UID}
