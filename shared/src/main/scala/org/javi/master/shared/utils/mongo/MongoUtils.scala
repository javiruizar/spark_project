package org.javi.master.shared.utils.mongo

import com.typesafe.config.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.javi.master.shared.config.ReadConfig.getOptionableConfig

object MongoUtils extends Logging {

  /**
   * Escribe el `DataFrame` en MongoDB usando los parámetros de `BatchConfig`.
   */
  def writeMongo(df: DataFrame, mongoConfig: MongoConfig): Unit = {
    log.info("Escribiendo datos en MongoDB...")

    val mongoUri = mongoConfig.outputUri.get
    val mongoDatabase = mongoConfig.outputDb.get
    val mongoCollection = mongoConfig.outputCollection.get
    try {

//      df.write.format("mongodb").mode(SaveMode.Overwrite).option("c", "")
      df.write
        .format("mongodb")
        .mode("append")
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

  def readMongo(spark: SparkSession, mongoConfig: MongoConfig): DataFrame = {
    log.info("Leyendo datos de MongoDB...")
    val mongoUri = mongoConfig.outputUri.get
    val mongoDatabase = mongoConfig.inputDb.get
    val mongoCollection = mongoConfig.inputCollection.get

    try {
      spark.read
        .format("mongodb")
        .option("connection.uri", mongoUri)
        .option("database", mongoDatabase)
        .option("collection", mongoCollection)
        .load()

    } catch {
      case e: Exception =>
        log.error(s"Error en la lectura en MongoDB: $mongoDatabase.$mongoCollection. ${e.getMessage}")
        throw e
    }
  }

  def getMongoConfig(conf: Config): MongoConfig = {

    val mongoConf = conf.getConfig("mongodb")
    MongoConfig(
      inputUri = getOptionableConfig(mongoConf, "input.uri"),
      outputUri = getOptionableConfig(mongoConf, "output.uri"),
      inputDb = getOptionableConfig(mongoConf, "input.database"),
      outputDb = getOptionableConfig(mongoConf, "output.database"),
      inputCollection = getOptionableConfig(mongoConf, "input.collection"),
      outputCollection = getOptionableConfig(mongoConf, "output.collection"),

    )
  }
}
