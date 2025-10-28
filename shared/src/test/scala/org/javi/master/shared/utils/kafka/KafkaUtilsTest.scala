package org.javi.master.shared.utils.kafka

import io.github.embeddedkafka.Codecs.{stringDeserializer, stringSerializer}
import io.github.embeddedkafka.EmbeddedKafka.{publishStringMessageToKafka, withRunningKafka}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigException
class KafkaUtilsTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll
  with BeforeAndAfterEach with EmbeddedKafka {


  "KafkaUtils.getKafkaConfig" should "load config when only inputTopic is defined" in {
    val configString =
      """
        |kafka {
        |  bootstrap.servers = "kafka-server:9092"
        |  topic.input = "mi-input-topic"
        |}
      """.stripMargin
    val conf = ConfigFactory.parseString(configString)

    // Asumo que tu 'getOptionableConfig' está en KafkaUtils o en el scope
    val kafkaConfig = KafkaUtils.getKafkaConfig(conf)

    kafkaConfig.bootstrapServers shouldBe "kafka-server:9092"
    kafkaConfig.inputTopic shouldBe Some("mi-input-topic")
    kafkaConfig.outputTopic shouldBe None
    kafkaConfig.startingOffset shouldBe None
  }

  it should "load config when only outputTopic is defined" in {
    val configString =
      """
        |kafka {
        |  bootstrap.servers = "kafka-server:9092"
        |  topic.output = "mi-output-topic"
        |}
      """.stripMargin
    val conf = ConfigFactory.parseString(configString)

    val kafkaConfig = KafkaUtils.getKafkaConfig(conf)

    kafkaConfig.bootstrapServers shouldBe "kafka-server:9092"
    kafkaConfig.inputTopic shouldBe None
    kafkaConfig.outputTopic shouldBe Some("mi-output-topic")
  }

  it should "load config when all fields are defined" in {
    val configString =
      """
        |kafka {
        |  bootstrap.servers = "kafka-server:9092"
        |  topic.input = "in-topic"
        |  topic.output = "out-topic"
        |  startingOffset = "earliest"
        |}
      """.stripMargin
    val conf = ConfigFactory.parseString(configString)

    val kafkaConfig = KafkaUtils.getKafkaConfig(conf)

    kafkaConfig.bootstrapServers shouldBe "kafka-server:9092"
    kafkaConfig.inputTopic shouldBe Some("in-topic")
    kafkaConfig.outputTopic shouldBe Some("out-topic")
    kafkaConfig.startingOffset shouldBe Some("earliest")
  }

  it should "throw  IllegalArgumentException when input and output topic are not defined" in {
    val configString =
      """
        |kafka {
        |  bootstrap.servers = "kafka-server:9092"
        |  // Sin topics
        |}
      """.stripMargin
    val conf = ConfigFactory.parseString(configString)

    // Comprobamos que el 'require' falla con la excepción correcta
    val exception = intercept[IllegalArgumentException] {
      KafkaUtils.getKafkaConfig(conf)
    }

    exception.getMessage should include ("At least 'kafka.topic.input or 'kafka.topic.output must be defined")
  }

  it should "throw ConfigException.Missing when bootstrap.server is not defined" in {
    val configString =
      """
        |kafka {
        |  topic.input = "in-topic"
        |  // Sin bootstrap.servers
        |}
      """.stripMargin
    val conf = ConfigFactory.parseString(configString)

    // Comprobamos que el .getString("bootstrap.servers") falla
    intercept[ConfigException.Missing] {
      KafkaUtils.getKafkaConfig(conf)
    }
  }

  it should "throw IllegalArgumentException when kafkablock is not defined" in {
    val configString =
      """
        |# Sin bloque kafka
        |otra.config {
        |  bootstrap.servers = "kafka-server:9092"
        |}
      """.stripMargin
    val conf = ConfigFactory.parseString(configString)

    // Comprobamos que el .getConfig("kafka") falla
    intercept[IllegalArgumentException] {
      KafkaUtils.getKafkaConfig(conf)
    }
  }

  // --- FIN DE LOS NUEVOS TESTS ---

  "KafkaUtils.readKafkaStream" should "read message from kafka topic correctly" in {
    val spark: SparkSession = SparkSession.builder()
      .appName("KafkaUtilsEmbeddedTest")
      .master("local[2]")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
      .getOrCreate()


    implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig()
    EmbeddedKafka.start()

    val topic = "input-topic"
    val bootstrapServers = s"localhost:${kafkaConfig.kafkaPort}"

    EmbeddedKafka.publishToKafka(topic, "key1", """{"field":"value1"}""")
    val cfg = KafkaConfig(
      bootstrapServers = bootstrapServers,
      inputTopic = Some(topic),
      outputTopic = None,
      startingOffset = Some("earliest")
    )

    // 4️⃣ Leer stream desde Kafka
    val df = KafkaUtils.readKafkaStream(spark, cfg)
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    // 5️⃣ Escribir en memoria para verificar
    val query: StreamingQuery = df.writeStream
      .format("memory")
      .queryName("kafka_test")
      .outputMode("append")
      .start()

    query.processAllAvailable()

    val results = spark.sql("SELECT * FROM kafka_test").collect()

    assert(results.nonEmpty, "No se leyó ningún mensaje desde Kafka")
    assert(results.exists(_.getAs[String]("key") == "key1"))
    assert(results.exists(_.getAs[String]("value").contains("value1")))

    // 6️⃣ Limpieza
    query.stop()
    EmbeddedKafka.stop()
    spark.stop()
  }

  "KafkaUtils.writeKafkaStream" should "write messages to Kafka correctly" in {

    val spark: SparkSession = SparkSession.builder()
      .appName("KafkaUtilsWriteTest")
      .master("local[2]")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
      .getOrCreate()

    implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig()
    EmbeddedKafka.start()

    val topic = "output-topic"
    val bootstrapServers = s"localhost:${kafkaConfig.kafkaPort}"

    import spark.implicits._
    val inputData = Seq(
      ("key1", """{"field":"value1"}"""),
      ("key2", """{"field":"value2"}""")
    ).toDF("key", "value")

    val cfg = KafkaConfig(
      bootstrapServers = bootstrapServers,
      inputTopic = None,
      outputTopic = Some(topic),
      startingOffset = None
    )

    KafkaUtils.writeKafkaStream(inputData, cfg)

    val consumedMessages =
      EmbeddedKafka.consumeNumberKeyedMessagesFrom[String, String](topic, 2, autoCommit = true).toMap

    consumedMessages("key1") should include ("value1")
    consumedMessages("key2") should include("value2")

    EmbeddedKafka.stop()
    spark.stop()
  }
  it should "handle empty DataFrame without writing messages" in {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import spark.implicits._
    implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig()
    EmbeddedKafka.start()

    val df = Seq.empty[(String, String)].toDF("key", "value")
    val topic = "empty-topic"

    val cfg = KafkaConfig(
      bootstrapServers = s"localhost:${kafkaConfig.kafkaPort}",
      inputTopic = None,
      outputTopic = Some(topic),
      startingOffset = None
    )

    KafkaUtils.writeKafkaStream(df, cfg)

    val consumed = EmbeddedKafka.consumeNumberKeyedMessagesFrom[String, String](topic, 0, autoCommit = true)
    consumed shouldBe empty

    EmbeddedKafka.stop()
    spark.stop()
  }

  it should "fail if Kafka bootstrap server is invalid" in {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import spark.implicits._

    val df = Seq(("key", "value")).toDF("key", "value")
    val cfg = KafkaConfig(
      bootstrapServers = "invalid-host:9999",
      inputTopic = None,
      outputTopic = Some("bad-topic"),
      startingOffset = None
    )

    intercept[org.apache.spark.SparkException] {
      KafkaUtils.writeKafkaStream(df, cfg)
    }

    spark.stop()
  }


}
