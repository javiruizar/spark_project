package org.javi.master.shared.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
class SparkSessionFactoryTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {


  private val testConfig = ConfigFactory.parseString(
    """
      |spark.master = "local[2]"
      |spark.app.name = "test-spark-session"
      |spark.ui.enabled = "false"
      |spark.driver.host = "localhost"
      |spark.sql.shuffle.partitions = "1"
      |spark.default.parallelism = "1"
      """.stripMargin)

  private lazy val spark: SparkSession = SparkSessionFactory.build(testConfig, "test-session")
  
//  override def afterAll(): Unit = {
//    if (spark != null) {
//      spark.stop()
//    }
//  }
  
  "SparkSessionFactory" should "create a SparkSession with provided configuration" in {

    spark should not be null
    spark.conf.get("spark.app.name") shouldBe "test-spark-session"
    spark.conf.get("spark.master") shouldBe "local[2]"
  }
  

  
  it should "apply all configurations from the provided config" in {

    spark.conf.get("spark.master") shouldBe "local[2]"
    spark.conf.get("spark.app.name") shouldBe "test-spark-session"
    spark.conf.get("spark.ui.enabled") shouldBe "false"
    spark.conf.get("spark.driver.host") shouldBe "localhost"
    spark.conf.get("spark.sql.shuffle.partitions") shouldBe "1"
    spark.conf.get("spark.default.parallelism") shouldBe "1"
  }
}
