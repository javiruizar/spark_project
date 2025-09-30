package org.javi.master.shared.utils.kafka

import com.typesafe.config.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaUtils extends Logging {

  def readKafkaStream(spark: SparkSession, cfg: Config): DataFrame = {
    val bootstrapServer = cfg.getString("kafka.bootstrap.servers")
    val topic = cfg.getString("kafka.topic.input")

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      //      .option("subscribe", "streaming-query")
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
  }

  def writeKafkaStream(df: DataFrame, cfg: Config): Unit = {
    val bootstrapServer = cfg.getString("kafka.bootstrap.servers")
    val topic = cfg.getString("kafka.topic.output")

    df
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("topic", topic)
      .save
  }
}
