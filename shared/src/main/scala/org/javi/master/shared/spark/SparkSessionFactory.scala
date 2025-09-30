package org.javi.master.shared.spark

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

/**
  * Fábrica centralizada para instanciar `SparkSession` a partir de un fichero de configuración
  * o de la configuración por defecto. Evita duplicar código en Batch y Streaming.
  */
object SparkSessionFactory {

  /**
    * Construye una `SparkSession`.
    *
    * @param appName  Nombre de la aplicación Spark.
    * @param conf   Configuración opcional (Typesafe Config). Si no se pasa se cargará `application.conf`.
    */
  def build(conf: Config, appName: String="Spark-App"): SparkSession = {

    val builder = SparkSession.builder().appName(appName)

    conf.entrySet().asScala.foreach { entry =>
      builder.config(entry.getKey, conf.getString(entry.getKey))
    }
    builder.getOrCreate()
  }

//  def getConfigFromFile(config:Config): Config = {
//   config.getConfig("spark-conf").entrySet().asScala
//  .map(entry => (entry.getKey, entry.getValue.unwrapped().toString))
//  .toMap
//  }
}
