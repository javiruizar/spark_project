package org.javi.master.shared.utils.json

import org.apache.spark.sql.SparkSession
import org.javi.master.shared.config.ReadConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JsonUtilsTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("JsonReaderTest")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
  private val conf = ReadConfig.load("shared/src/test/resources/conf/batch.conf")


//  override def afterAll(): Unit = {
//    if (spark != null) spark.stop()
//  }

  "JsonUtils.getFinalPath" should "obtain relative path in local mode" in {
    val path = JsonUtils.getFinalPath(spark, conf)

    path shouldBe "data/json/input/"
  }
  "JsonUtils.getFinalPath" should "obtain absolute paths in cluster mode" in {

    spark.conf.set("spark.master", "yarn")


    val path = JsonUtils.getFinalPath(spark, conf)

    path shouldBe "/data/json/input/"
  }

  "JsonUtils.read" should "read JSON file with relative path in local mode" in {
    // Crear un archivo JSON temporal para la prueba

    try {
      val df = JsonUtils.readJson(spark, "shared/src/test/resources/json/batch.json")

      df.count() shouldBe 2
      df.columns should contain allOf("id", "name")
    }
  }

  "JsonUtils.read" should "throw an exception for non-existent file" in {
    an[Exception] should be thrownBy {
      JsonUtils.readJson(spark, "/path/that/does/not/exist.json")
    }
  }

}
