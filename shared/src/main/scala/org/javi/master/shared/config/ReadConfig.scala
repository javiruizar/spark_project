package org.javi.master.shared.config

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader
import org.apache.spark.internal.Logging

import java.io.File
import scala.io.Source

object ReadConfig extends Logging {
  def load(confPath: String): Config = {
    try {
      val conf: Config =
        if (confPath.startsWith("hdfs://")) {
          val hadoopConf = new Configuration()
          val fs = FileSystem.get(new java.net.URI(confPath), hadoopConf)
          val path = new Path(confPath)
          val in = fs.open(path)
          try {
            ConfigFactory.parseReader(new InputStreamReader(in, "UTF-8")).resolve()
          } finally {
            in.close()
          }
        } else
        {
          val customConfigFile = new File(confPath)
          val fileContentAsString = Source.fromFile(customConfigFile).getLines().mkString("\n")

          ConfigFactory.parseString(fileContentAsString)
        }
      conf
    } catch {
      case e: Exception =>
        log.error(s"Error en la lectura del conf $confPath}. ${e.getMessage}")
        throw e
    }
  }

  def getOptionableConfig(conf: Config, path: String): Option[String] = {
    try {
      if (conf.hasPath(path)) Some(conf.getString(path)) else None
    }
    catch {
      case e: Exception =>
        log.error(s"Error parseando la configuracion $path. " +
          s"Revisar si el fichero $conf la contiene y esta correctamente definida como Option[String]. ${e.getMessage}")
        throw e
    }
}
}
