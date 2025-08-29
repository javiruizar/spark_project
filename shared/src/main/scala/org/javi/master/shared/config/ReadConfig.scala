package org.javi.master.shared.config

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader
import org.apache.spark.internal.Logging

object ReadConfig extends Logging {
  def load(confPath: String): Config = {
    try {
      val conf : Config =
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
      } else {
        ConfigFactory.parseFile(new java.io.File(confPath))
      }
      val renderOptions = ConfigRenderOptions.defaults()
        .setOriginComments(false) // No mostrar comentarios sobre el origen de cada valor
        .setComments(false)       // No mostrar comentarios del fichero
        .setJson(true)            // Usar formato JSON
        .setFormatted(true)
      println(conf.root().render(renderOptions))
      conf
    } catch {
      case e: Exception =>
        log.error(s"Error en la lectura del conf $confPath}. ${e.getMessage}")
        throw e
    }

  }
}
