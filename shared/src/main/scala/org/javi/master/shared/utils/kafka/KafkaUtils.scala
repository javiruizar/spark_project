package org.javi.master.shared.utils.kafka

import com.typesafe.config.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.javi.master.shared.config.ReadConfig.getOptionableConfig

object KafkaUtils extends Logging {

  def getKafkaConfig(conf: Config): KafkaConfig = {
    require(
      conf.hasPath("kafka"),
      "kafka config block does not exist in config file. getKafkaConfig will not work unless it is defined."
    )
    val kafkaConf = conf.getConfig("kafka")
    val config: KafkaConfig = KafkaConfig(
      bootstrapServers = kafkaConf.getString("bootstrap.servers"),
      inputTopic = getOptionableConfig(kafkaConf, "topic.input"),
      outputTopic = getOptionableConfig(kafkaConf, "topic.output"),
      startingOffset = getOptionableConfig(kafkaConf, "startingOffset")
    )
    require(
      config.inputTopic.isDefined || config.outputTopic.isDefined,
      "Invalid Kafka config. At least 'kafka.topic.input or 'kafka.topic.output must be defined in conf."
    )
    config

  }

  def readKafkaStream(spark: SparkSession, cfg: KafkaConfig): DataFrame = {
    require(
      cfg.inputTopic.isDefined,
      "Invalid Kafka config. 'kafka.topic.input' must be defined for readKafkaStream invocation."
    )
    val bootstrapServer = cfg.bootstrapServers
    val topic = cfg.inputTopic.get
    val startingOffset = cfg.startingOffset.get
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffset)
      .load()
  }

  def writeKafkaStream(df: DataFrame, cfg: KafkaConfig): Unit = {
    require(
      cfg.outputTopic.isDefined,
      "Invalid Kafka config. 'kafka.topic.output' must be defined for writeKafkaStream invocation."
    )
    val bootstrapServer = cfg.bootstrapServers
    val topic = cfg.outputTopic.get

    df
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("topic", topic)
      .save
  }

}
