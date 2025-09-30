package org.javi.master.shared.utils.mongo

import com.typesafe.config.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object MongoUtils extends Logging {

  /**
    * Escribe el `DataFrame` en MongoDB usando los parámetros de `BatchConfig`.
    */
  def writeMongo(df: DataFrame, cfg: Config): Unit = {
    log.info("Escribiendo datos en MongoDB...")
    val mongoDatabase= cfg.getString("spark.mongodb.output.database")
    val mongoUri = cfg.getString("spark.mongodb.output.uri")
    val mongoCollection = cfg.getString("spark.mongodb.output.collection")
    try {
      df.write
        .format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongoUri) // Se puede obviar ya que en la sparkSession se define la conf por defecto de mongo
        .option("database", mongoDatabase)
        .option("collection", mongoCollection)
        .save()

      log.info(s"Escritura en $mongoDatabase.$mongoCollection completada con exito.")
    } catch {
      case e: Exception =>
        log.error(s"Error en la escritura en MongoDB: $mongoDatabase.$mongoCollection. ${e.getMessage}")
        throw e
    }
  }

  def readMongo (spark: SparkSession, cfg: Config): DataFrame = {
    log.info("Leyendo datos de MongoDB...")
    val mongoDatabase= cfg.getString("spark.mongodb.input.database")
    val mongoCollection = cfg.getString("spark.mongodb.input.collection")

    try {
      spark.read
        .format("mongodb")
        .option("database", mongoDatabase)
        .option("collection", mongoCollection)
        .load()

    } catch {
      case e: Exception =>
        log.error(s"Error en la lectura en MongoDB: $mongoDatabase.$mongoCollection. ${e.getMessage}")
        throw e
    }
  }
}
