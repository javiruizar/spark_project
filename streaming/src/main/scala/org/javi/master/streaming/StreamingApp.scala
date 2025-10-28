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

object StreamingApp extends Logging {

  def main(args: Array[String]): Unit = {

    val confPath = System.getProperty("config.file")
    val config: Config = ReadConfig.load(confPath)
    val mongoConfig: MongoConfig = getMongoConfig(config)
    val kafkaConfig: KafkaConfig = getKafkaConfig(config)

    val streamingBuilder = getAllConfigforSpark(config)
    val spark: SparkSession = buildSparkSession(streamingBuilder)

    import spark.implicits._


    val mongoData = readMongo(spark, mongoConfig)

    val concatFieldOfSellingFeatures = mongoData.concatenateNestedFieldsWithValues("caracteristicas_venta")

    val allArticlesDataFrame = mongoData.getAllArticlesWithFeaturesDf(concatFieldOfSellingFeatures)

    val kafkaDF = readKafkaStream(spark, kafkaConfig)
      .selectExpr("CAST(value AS STRING) as BUSQUEDA")

//    val inputConsole = kafkaDF.writeStream.format("console")
//      .option("truncate", false)
//      .outputMode("append")
//      .start()

    kafkaDF
      .writeStream
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
        if (batchDF.count() == 1) {
          val busqueda = batchDF.select("BUSQUEDA").collect()(0).mkString.replace("\"", "")
            .toLowerCase.split(" ")

          val output = allArticlesDataFrame.getFoundArticlesDf(busqueda)

          val dummyData = Seq("No se ha encontrado ningun artículo con esas palabras clave")
          val noSuchArticleMessage = spark.sparkContext.parallelize(dummyData).toDF("value")

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
