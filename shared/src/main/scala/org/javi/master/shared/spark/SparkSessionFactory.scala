package org.javi.master.shared.spark

import com.typesafe.config.{Config, ConfigFactory}
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
    * @param config   Configuración opcional (Typesafe Config). Si no se pasa se cargará `application.conf`.
    */
  def build(appName: String, conf: Config): SparkSession = {

    val builder      = SparkSession.builder().appName(appName)

    // Aplica todas las key/values del fichero de configuración
    conf.entrySet().asScala.foreach { entry =>
      builder.config(entry.getKey, conf.getString(entry.getKey))
    }
    builder.getOrCreate()
  }
}
