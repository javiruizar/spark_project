package org.javi.master.shared.utils.kafka

case class KafkaConfig(
                        bootstrapServers: String,
                        inputTopic: Option[String] = None,
                        outputTopic: Option[String] = None,
                        startingOffset: Option[String] = Some("earliest")
                      )
