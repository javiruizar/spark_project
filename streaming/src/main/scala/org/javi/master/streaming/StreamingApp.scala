package org.javi.master.streaming


import com.typesafe.config.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.javi.master.shared.config.ReadConfig
import org.javi.master.shared.spark.SparkSessionFactory._
import org.javi.master.shared.utils.mongo.MongoUtils.{getMongoConfig, readMongo}
import org.javi.master.shared.ops.DataFrameOps._
import org.javi.master.shared.utils.kafka.KafkaUtils.{getKafkaConfig, readKafkaStream, writeKafkaStream}
import org.javi.master.shared.utils.kafka.KafkaConfig
import org.javi.master.shared.utils.mongo.MongoConfig

import java.io.File
//import org.javi.master.streaming.processing.QueryProcessor

object StreamingApp extends Logging {

  def main(args: Array[String]): Unit = {

    val confPath = System.getProperty("config.file")
    val config: Config = ReadConfig.load(confPath)
    val stramingBuilder = getAllConfigforSpark(config)
    val ssc: SparkSession = buildSparkSession(stramingBuilder)

    import ssc.implicits._

    val mongoConfig: MongoConfig = getMongoConfig(config)
    val mongoData = readMongo(ssc, mongoConfig)

    val sellingFeatures = mongoData.getFieldsOfNestedColumn("caracteristicas_venta")

    val allArticlesDataFrame = mongoData.getAllArticlesWithFeaturesDf(sellingFeatures)

    log.info("LEYENDO DE KAFKA")

    val kafkaConfig: KafkaConfig = getKafkaConfig(config)
    val kafkaDF = readKafkaStream(ssc, kafkaConfig)
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

              writeKafkaStream(noSuchArticleMessage, kafkaConfig)
            case _ =>

              log.info(s"Se han encontrado ${output.count()} articulos que podrian interesarte:")
              writeKafkaStream(output, kafkaConfig)
          }
        }
      }
      .start()
      .awaitTermination()
//    inputConsole.awaitTermination()

  }

}
