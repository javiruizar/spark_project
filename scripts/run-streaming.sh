#!/usr/bin/env bash

ENVIRONMENT=${1:-dev}
case $ENVIRONMENT in
  prod)
    SPARK_MASTER="yarn"
    JAR_PATH="$(dirname "$BASH_SOURCE[0]")/../lib/streaming-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
    ;;
  *) # Para cualquier otro caso (dev, qa, etc.)
    SPARK_MASTER="local[*]"
    JAR_PATH="$(dirname "$BASH_SOURCE[0]")/streaming/target/streaming-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
    ;;
esac
echo $JAR_PATH

CONFIG_FILE="$(dirname "$BASH_SOURCE[0]")/conf/$ENVIRONMENT/streaming.conf"
# Configuración por defecto
#CONFIG_FILE="${CONFIG_FILE:-$(dirname "$BASH_SOURCE[0]")/conf/$ENVIRONMENT/streaming.conf}"
LOG2J_FILE="${LOG2J_FILE:-$(dirname  "$BASH_SOURCE[0]")/conf/$ENVIRONMENT/log4j2.properties}"
echo $LOG2J_FILE
# Ejecutar spark-submit
spark-submit \
  --class org.javi.master.streaming.StreamingApp \
  --master "$SPARK_MASTER" \
  --conf "spark.driver.extraJavaOptions=-Dconfig.file=$CONFIG_FILE -Dlog4j.configurationFile=$(dirname "$0")/../conf/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties" \
  --files "$CONFIG_FILE,$LOG2J_FILE" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:10.1.0 \
  "$JAR_PATH" "$@"
