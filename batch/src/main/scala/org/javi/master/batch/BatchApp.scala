package org.javi.master.batch

import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.javi.master.shared.config.ReadConfig
import org.javi.master.shared.spark.SparkSessionFactory
import org.javi.master.shared.spark.SparkSessionFactory.{buildSparkSession, getAllConfigforSpark}
import org.javi.master.shared.utils.json.JsonUtils._
import org.javi.master.shared.utils.mongo.MongoUtils.getMongoConfig
import org.javi.master.shared.utils.mongo.{MongoConfig, MongoUtils}

/**
  * Punto de entrada de la aplicación Batch — ahora solo orquesta componentes.
  */
object BatchApp extends Logging {

  def main(args: Array[String]): Unit = {
    log.info("STARTING PROCESS")
    val confPath = System.getProperty("config.file")
    val config: Config = ReadConfig.load(confPath)
//    val renderOptions = ConfigRenderOptions.defaults()
//      .setOriginComments(false) // No mostrar comentarios sobre el origen de cada valor
//      .setComments(false)       // No mostrar comentarios del fichero
//      .setJson(true)            // Usar formato JSON
//      .setFormatted(true)
//    println(config.root().render(renderOptions))
    val batchBuilder: SparkSession.Builder = getAllConfigforSpark(config)
    val spark: SparkSession = buildSparkSession(batchBuilder, "ElMercado-BatchApplication")

    val mongoConfig: MongoConfig = getMongoConfig(config)
    try {
      val jsonPath = getFinalPath(spark, config)
      val inputDf = readJson(spark, jsonPath)
      // Filtra columnas relevantes
      val selected = inputDf.select("id_articulo", "nombre_articulo", "palabras_clave", "caracteristicas_venta")
      MongoUtils.writeMongo(selected, mongoConfig)
      log.info("Proceso Batch finalizado con éxito")
    } catch {
      case e: Exception =>
        log.error("Error durante la ejecucion Batch", e)
        throw e
    } finally {
      spark.stop()
    }
  }
}
