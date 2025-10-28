#!/usr/bin/env bash

ENVIRONMENT=${1:-dev}
case $ENVIRONMENT in
  prod)
    SPARK_MASTER="yarn"
    JAR_PATH="$(dirname  "$BASH_SOURCE[0]")/../lib/batch-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
    ;;
  *) # Para cualquier otro caso (dev, qa, etc.)
    SPARK_MASTER="local[*]"
    JAR_PATH="$(dirname "$BASH_SOURCE[0]")/batch/target/batch-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
    ;;
esac

# Configuración por defecto
CONFIG_FILE="$(dirname  "$BASH_SOURCE[0]")/conf/$ENVIRONMENT/batch.conf"
#CONFIG_FILE="${CONFIG_FILE:-$(dirname  "$BASH_SOURCE[0]")/../conf/$ENVIRONMENT/batch.conf}" # ${VAR:-DEFAULT} si existe VAR usala, si no usa DEFAULT
#:- → usa default si no está definida.
#:= → usa default y además asigna.
#:+ → sustituye solo si está definida.
#:? → aborta si no está definida.
LOG2J_FILE="$(dirname  "$BASH_SOURCE[0]")/conf/$ENVIRONMENT/log4j2.properties"
echo $LOG2J_FILE
echo $JAR_PATH
echo $SPARK_MASTER
spark-submit \
  --class org.javi.master.batch.BatchApp \
  --master "$SPARK_MASTER" \
  --files "$CONFIG_FILE,$LOG2J_FILE" \
  --conf "spark.driver.extraJavaOptions=-Dconfig.file=$CONFIG_FILE -Dlog4j.configurationFile=$(dirname "$0")/../conf/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=./conf/dev/log4j2.properties -Dlog4j.debug=true"\
  "$JAR_PATH" "$@"
