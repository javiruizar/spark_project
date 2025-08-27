package org.javi.master.streaming.processing

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{array_intersect, col, concat, concat_ws, lit, max, size}
import org.apache.spark.sql.types.StringType

/**
  * Contiene la lógica de filtrado de artículos frente a las palabras clave buscadas.
  */
object QueryProcessor {

  /**
    * Filtra `articles` según las palabras clave y devuelve un `DataFrame` con la columna `value` preparada para Kafka.
    */
  def process(articles: DataFrame, busqueda: Array[String]): DataFrame = {
    val spark = articles.sparkSession
    import spark.implicits._

    articles
      .withColumn("busqueda", lit(busqueda))
      .withColumn("inters_size", size(array_intersect(col("busqueda"), col("palabras_clave"))))
      .filter(col("inters_size") > 0)
      .withColumn("max_value", max("inters_size").over())
      .filter(col("max_value") === col("inters_size"))
      .select(col("valores").cast(StringType).as("value"))
  }
}
