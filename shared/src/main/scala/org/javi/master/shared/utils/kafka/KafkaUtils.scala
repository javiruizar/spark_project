package org.javi.master.shared.utils.kafka

import com.typesafe.config.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.javi.master.shared.config.ReadConfig.getOptionableConfig

object KafkaUtils extends Logging {

  def readKafkaStream(spark: SparkSession, cfg: KafkaConfig): DataFrame = {
    val bootstrapServer = cfg.bootstrapServers
    val topic = cfg.inputTopic.get

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
  }

  def writeKafkaStream(df: DataFrame, cfg: KafkaConfig): Unit = {
    val bootstrapServer = cfg.bootstrapServers
    val topic = cfg.outputTopic.get

    df
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("topic", topic)
      .save
  }

  def getKafkaConfig(conf: Config): KafkaConfig = {

    val kafkaConf = conf.getConfig("kafka")
    KafkaConfig(
      kafkaConf.getString("bootstrap.servers"),
      inputTopic = getOptionableConfig(kafkaConf, "topic.input"),
      outputTopic = getOptionableConfig(kafkaConf, "topic.output")

    )
  }
}
