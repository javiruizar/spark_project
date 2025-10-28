package org.javi.master.shared.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.javi.master.shared.spark.SparkSessionFactory.{buildSparkSession, getAllConfigforSpark}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkSessionFactoryTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var spark: SparkSession = _


  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterEach()
  }


  "getAllConfigforSpark" should "build a SparkSession.Builder with custom configurations" in {
    val configStr =
      """
        |spark {
        |  master = "local[*]"
        |  sql.shuffle.partitions = "5"
        |  ui.enabled = "false"
        |}
        |""".stripMargin

    val conf = ConfigFactory.parseString(configStr)

    val builder = getAllConfigforSpark(conf)
    spark = buildSparkSession(builder, "TestApp")

    spark.sparkContext.master shouldBe "local[*]"
    spark.conf.get("spark.sql.shuffle.partitions") shouldBe "5"
    spark.conf.get("spark.ui.enabled") shouldBe "false"
    spark.sparkContext.appName shouldBe "TestApp"
  }

  it should "not fail if spark section is missing" in {
    val conf = ConfigFactory.parseString(
      """
        |app.name = "NoSparkConfig"
        |""".stripMargin)

    noException should be thrownBy {
      val builder = getAllConfigforSpark(conf)
      spark = buildSparkSession(builder, "DefaultApp")
    }

    spark.sparkContext.appName shouldBe "DefaultApp"
  }

  "buildSparkSession" should "create a SparkSession even with minimal builder" in {
    val builder = SparkSession.builder().master("local[*]")
    spark = buildSparkSession(builder, "MinimalApp")

    spark should not be null
    spark.sparkContext.appName shouldBe "MinimalApp"
    spark.sparkContext.master shouldBe "local[*]"
  }
}
