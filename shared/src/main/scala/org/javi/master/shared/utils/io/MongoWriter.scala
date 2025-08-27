package org.javi.master.shared.utils.io

import com.typesafe.config.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

object MongoWriter extends Logging {

  /**
    * Escribe el `DataFrame` en MongoDB usando los parámetros de `BatchConfig`.
    */
  def write(df: DataFrame, cfg: Config): Unit = {
    log.info("Escribiendo datos en MongoDB...")
    val mongoDb= cfg.getString("spark.mongodb.output.database")
    val mongoUri = cfg.getString("spark.mongodb.output.uri")
    val mongoCollection = cfg.getString("spark.mongodb.output.collection")
    try {

      df.write
        .format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongoUri)
        .option("database", mongoDb)
        .option("collection", mongoCollection)
        .save()

      log.info(s"Escritura en $mongoDb.$mongoCollection completada con exito.")
    } catch {
      case e: Exception =>
        log.error(s"Error en la escritura en $mongoDb.$mongoCollection. ${e.getMessage}")
        throw e
    }
  }
}
