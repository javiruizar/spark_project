package org.javi.master.shared.utils.json

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonUtils {

  def getFinalPath(spark: SparkSession, config: Config): String = {
    val sparkMaster = spark.conf.get("spark.master")
    sparkMaster match {
      case "local[*]" => config.getString("inputRelativePath")
      case _           => s"/${config.getString("inputRelativePath")}"
    }
  }
  /**
    * Lectura genérica de ficheros JSON en modo multiline.
    *
    * @param spark `SparkSession` existente.
    * @param path  Ruta al directorio o fichero JSON.
    */
  def readJson(spark: SparkSession, path: String): DataFrame =
    spark.read.option("multiline", "true").json(path)
}
