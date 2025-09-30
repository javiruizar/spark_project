package org.javi.master.streaming


import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{array_intersect, col, concat, concat_ws, lit, max, size, when}
import org.apache.spark.sql.types.{StringType, StructType}
import org.javi.master.shared.config.ReadConfig
import org.javi.master.shared.spark.SparkSessionFactory
import org.javi.master.shared.utils.mongo.MongoUtils.readMongo
import org.javi.master.shared.ops.DataFrameOps._
import org.javi.master.shared.utils.kafka.KafkaUtils.{readKafkaStream, writeKafkaStream}

import java.io.File
//import org.javi.master.streaming.processing.QueryProcessor

object StreamingApp extends Logging {

  def main(args: Array[String]): Unit = {

    val confPath = System.getProperty("config.file")
    val cfg: Config = ReadConfig.load(confPath)
    val ssc: SparkSession = SparkSessionFactory.build(cfg)

    import ssc.implicits._

    val mongoData = readMongo(ssc, cfg)

    val sellingFeatures = mongoData.getFieldsOfNestedColumn("caracteristicas_venta")

    val allArticlesDataFrame = mongoData.getAllArticlesWithFeaturesDf(sellingFeatures)

    log.info("LEYENDO DE KAFKA")

    val kafkaDF = readKafkaStream(ssc, cfg)
      .selectExpr("CAST(value AS STRING) as BUSQUEDA")

//    val inputConsole = kafkaDF.writeStream.format("console")
//      .option("truncate", false)
//      .outputMode("append")
//      .start()

    log.info("ESCRIBIENDO EN KAFKA")

    kafkaDF
      .writeStream
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
        if (batchDF.count() == 1) {
          val busqueda = batchDF.select("BUSQUEDA").collect()(0).mkString.replace("\"", "")
            .toLowerCase.split(" ")

          val output = allArticlesDataFrame.getFoundArticlesDf(busqueda)

          val dummyData = Seq("No se ha encontrado ningun artículo con esas palabras clave")
          val noSuchArticleMessage = ssc.sparkContext.parallelize(dummyData).toDF("value")

          output.show(10)

          output.count() match {
            case 0 =>
              log.warn("No se ha encontrado ningun articulo.")

              writeKafkaStream(noSuchArticleMessage, cfg)
            case _ =>

              log.info(s"Se han encontrado ${output.count()} articulos que podrian interesarte:")
              writeKafkaStream(output, cfg)
          }
        }
      }
      .start()
      .awaitTermination()
//    inputConsole.awaitTermination()

  }

}
