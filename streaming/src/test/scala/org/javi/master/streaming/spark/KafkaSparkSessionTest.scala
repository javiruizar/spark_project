//package org.javi.master.streaming.spark
//
//import org.apache.spark.sql.SparkSession
//import org.javi.master.streaming.config.StreamingConfig
//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.{BeforeAndAfterAll}
//import org.scalatest.matchers.should.Matchers
//
//class KafkaSparkSessionTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
//
//  private var spark: SparkSession = _
//
//  private val testConfig = StreamingConfig(
//    mongoUri = "mongodb://test:27017",
//    mongoDatabase = "testdb",
//    mongoCollection = "testcoll",
//    checkpointLocation = "/tmp/checkpoint",
//    kafkaLocalBootstrap = "localhost:9092",
//    kafkaClusterBootstrap = "kafka:9092"
//  )
//
//  override def beforeAll(): Unit = {
//    // No need to create a session here, let the test create it
//  }
//
//  override def afterAll(): Unit = {
//    if (spark != null) {
//      spark.stop()
//    }
//  }
//
//  "KafkaSparkSession.buildSparkSession" should "create a SparkSession with correct configuration" in {
//    spark = KafkaSparkSession.buildSparkSession(testConfig)
//
//    spark should not be null
//    spark.conf.get("spark.mongodb.read.connection.uri") shouldBe testConfig.mongoUri
//    spark.conf.get("spark.sql.streaming.checkpointLocation") shouldBe testConfig.checkpointLocation
//    spark.conf.get("spark.jars.packages") should include ("mongo-spark-connector")
//  }
//
//  it should "set memory configurations correctly" in {
//    spark = KafkaSparkSession.buildSparkSession(testConfig)
//
//    spark.conf.get("spark.driver.memory") shouldBe "1g"
//    spark.conf.get("spark.executor.memory") shouldBe "1g"
//  }
//
//  it should "set the application name correctly" in {
//    spark = KafkaSparkSession.buildSparkSession(testConfig)
//
//    spark.sparkContext.appName shouldBe "ElMercado-StreamingApplication"
//  }
//
//  it should "use the same session for multiple calls with same config" in {
//    val spark1 = KafkaSparkSession.buildSparkSession(testConfig)
//    val spark2 = KafkaSparkSession.buildSparkSession(testConfig)
//
//    try {
//      spark1 should be theSameInstanceAs spark2
//    } finally {
//      spark1.stop()
//      // Don't stop spark2 as it's the same instance as spark1
//    }
//  }
//}
