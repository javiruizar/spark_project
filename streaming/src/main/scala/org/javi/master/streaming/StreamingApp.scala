package org.javi.master.streaming


import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{array_intersect, col, concat, concat_ws, max, lit, size, when}
import org.apache.spark.sql.types.{StringType, StructType}


import org.javi.master.streaming.config.StreamingConfig
import org.javi.master.streaming.spark.KafkaSparkSession
import org.javi.master.streaming.processing.QueryProcessor

object StreamingApp extends Logging {

  def main(args: Array[String]): Unit = {
    val cfg   = StreamingConfig.load("conf/streaming.conf")
    val ssc   = KafkaSparkSession.build(cfg)

    import ssc.implicits._

    val bootstrapServer = if (ssc.conf.get("spark.master") == "local[*]") cfg.kafkaLocalBootstrap else cfg.kafkaClusterBootstrap

    val mongoData = ssc.read
      .format("mongodb")
      .load()

    val caracteristicas_venta = mongoData.select("caracteristicas_venta").schema.fields.head.dataType.asInstanceOf[StructType].fields

    val keyValueColumns = caracteristicas_venta.map { field =>
      val colName = field.name
      val colValue = col(s"caracteristicas_venta.$colName")
      when(colValue.isNotNull, concat_ws(", ", concat_ws(":", lit(colName),colValue)))
    }

    val articlesDataFrame = mongoData
      .withColumn("clave_valor", concat_ws(", ", keyValueColumns: _*))
      .withColumn("valores", concat(lit("Articulo: "), col("nombre_articulo"), lit("\nCaracteristicas: \n"), col("clave_valor")))
      .select("nombre_articulo", "palabras_clave", "valores")

    val kafkaDF = ssc.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", "streaming-query")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as BUSQUEDA")

    kafkaDF
      .writeStream
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
        if (batchDF.count() == 1) {
          val busqueda = batchDF.select("BUSQUEDA").collect()(0).mkString.replace("\"", "")
            .toLowerCase.split(" ")

          val output = QueryProcessor.process(articlesDataFrame, busqueda)


          val dummyData = Seq("No se ha encontrado ningun artículo con esas palabras clave")
          val noSuchArticleMessage = ssc.sparkContext.parallelize(dummyData).toDF("value")

          output.count() match {
            case 0 =>
              println("No se ha encontrado ningun articulo")
              noSuchArticleMessage
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("topic", "output")
                .save
            //              ()
            case _ =>
              println(s"Se han encontrado ${output.count()} articulos que podrian interesarte:")
              output
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("topic", "output")
                .save
          }
          println("saved results")
        }
      }
      .start().awaitTermination()
  }
}
