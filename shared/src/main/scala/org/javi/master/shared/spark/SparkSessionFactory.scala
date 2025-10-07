package org.javi.master.shared.spark

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import org.javi.master.shared.config.ReadConfig.getOptionableConfig

import scala.collection.JavaConverters._

/**
 * Fábrica centralizada para instanciar `SparkSession` a partir de un fichero de configuración
 * o de la configuración por defecto. Evita duplicar código en Batch y Streaming.
 */
object SparkSessionFactory {

  /**
   * Construye una `SparkSession`.
   *
   * @param sparkBuilder Builder creado a partir de la config del fichero de configuracion de la app
   * @param appName      Nombre de la aplicación Spark.
   */
  def buildSparkSession(sparkBuilder: Builder, appName: String = "Spark-App"): SparkSession = {

    sparkBuilder
      .appName(appName)
      .getOrCreate()
  }

  def getAllConfigforSpark(conf: Config): Builder = {

    val builder = SparkSession.builder()
    if (conf.hasPath("spark")) {
        conf.getConfig("spark").entrySet().asScala.foreach { entry =>
          builder.config(entry.getKey, conf.getString(entry.getKey))
        }
    }
    builder
  }

}
