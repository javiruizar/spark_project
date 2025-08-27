//package org.javi.master.streaming.spark
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.SparkConf
//import org.javi.master.streaming.config.StreamingConfig
//import org.apache.spark.internal.Logging
//
//object KafkaSparkSession extends Logging {
//  def build(cfg: StreamingConfig): SparkSession = {
//    val conf = new SparkConf()
//      .set("spark.mongodb.read.connection.uri", cfg.mongoUri)
//      .set("spark.sql.streaming.checkpointLocation", cfg.checkpointLocation)
//      .set("spark.driver.memory", "1g")
//      .set("spark.executor.memory", "1g")
//      .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.0")
//
//    log.info("Creando SparkSession para streaming…")
//    SparkSession
//      .builder()
//      .config(conf)
//      .appName("ElMercado-StreamingApplication")
//      .getOrCreate()
//  }
//}
