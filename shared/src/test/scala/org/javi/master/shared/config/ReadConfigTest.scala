package org.javi.master.shared.config

import java.nio.file.{Files, Paths}
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReadConfigTest extends AnyFlatSpec with Matchers {

  it should "load configuration from a custom config file" in {

    val conf = ReadConfig.load("shared/src/test/resources/conf/batch.conf")

    conf.getString("inputRelativePath") shouldBe "data/json/input/"
    conf.getString("spark.mongodb.output.uri") shouldBe "mongodb://localhost:27017"
    conf.getString("spark.mongodb.output.database") shouldBe "elmercado"
    conf.getString("spark.mongodb.output.collection") shouldBe "articulos"

  }

  it should "throw an exception for incorrect required path" in {

    an[Exception] should be thrownBy {
      ReadConfig.load("incorrect/path").getString("incorrect.value")
    }
  }
}
