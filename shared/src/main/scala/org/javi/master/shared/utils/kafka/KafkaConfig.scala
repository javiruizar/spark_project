package org.javi.master.shared.utils.kafka

case class KafkaConfig(
                        bootstrapServers: String,
                        inputTopic: Option[String],
                        outputTopic: Option[String]
                      )
