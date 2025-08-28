#!/usr/bin/env bash

# Configuración por defecto
CONFIG_FILE="${CONFIG_FILE:-$(dirname "$0")/../conf/batch.conf}" # ${VAR:-DEFAULT} si existe VAR usala, si no usa DEFAULT
#:- → usa default si no está definida.
#:= → usa default y además asigna.
#:+ → sustituye solo si está definida.
#:? → aborta si no está definida.

JAR_PATH="${JAR_PATH:-$(dirname "$0")/../lib/batch-1.0.0-SNAPSHOT-jar-with-dependencies.jar}"

spark-submit \
  --class org.javi.master.batch.BatchApp \
  --master yarn \
  --deploy-mode cluster \
  --conf "spark.driver.extraJavaOptions=-Dconfig.file=$CONFIG_FILE -Dlog4j.configurationFile=$(dirname "$0")/../conf/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties" \
  --files "$CONFIG_FILE","$(dirname "$0")/../conf/log4j2.properties" \
  "$JAR_PATH" "$@"
