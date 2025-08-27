package org.javi.master.shared.config

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.apache.spark.internal.Logging

object ReadConfig extends Logging {
  def load(confPath: String): Config = {
    try {
      val conf: Config = ConfigFactory.parseFile(new java.io.File(confPath))
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
