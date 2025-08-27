#!/usr/bin/env bash

# Configuración por defecto
CONFIG_FILE="${CONFIG_FILE:-$(dirname "$0")/../conf/batch.conf}"
JAR_PATH="${JAR_PATH:-$(dirname "$0")/../batch/target/batch-1.0.0-SNAPSHOT-jar-with-dependencies.jar}"

# Ejecutar spark-submit
spark-submit \
  --class org.javi.master.batch.BatchApp \
  --master yarn \
  --conf "spark.driver.extraJavaOptions=-Dconfig.file=$CONFIG_FILE -Dlog4j.configurationFile=$(dirname "$0")/../conf/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties" \
  --files "$CONFIG_FILE","$(dirname "$0")/../conf/log4j2.properties" \
  "$JAR_PATH" "$@"
