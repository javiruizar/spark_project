#!/usr/bin/env bash

# Configuración por defecto
SPARK_MASTER=${SPARK_MASTER:-"local[*]"}
CONFIG_FILE="${CONFIG_FILE:-$(dirname "$0")/../conf/streaming.conf}"
JAR_PATH="${JAR_PATH:-$(dirname "$0")/../streaming/target/streaming-1.0.0-SNAPSHOT-jar-with-dependencies.jar}"

# Ejecutar spark-submit
spark-submit \
  --class org.javi.master.streaming.StreamingApp \
  --master "$SPARK_MASTER" \
  --conf "spark.driver.extraJavaOptions=-Dconfig.file=$CONFIG_FILE -Dlog4j.configurationFile=$(dirname "$0")/../conf/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties" \
  --files "$CONFIG_FILE","$(dirname "$0")/../conf/log4j2.properties" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:10.1.0 \
  "$JAR_PATH" "$@"
