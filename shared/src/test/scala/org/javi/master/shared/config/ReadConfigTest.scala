package org.javi.master.shared.config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.mockito.ArgumentMatchers.any
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito.{mock,mockStatic,when}

import java.net.URI

class ReadConfigTest extends AnyFlatSpec with Matchers {

  it should "load: load configuration from a custom config file" in {

    val conf = ReadConfig.load("shared/src/test/resources/conf/batch.conf")

    conf.getString("inputRelativePath") shouldBe "data/json/input/"
    conf.getString("spark.mongodb.output.uri") shouldBe "mongodb://localhost:27017"
    conf.getString("spark.mongodb.output.database") shouldBe "elmercado"
    conf.getString("spark.mongodb.output.collection") shouldBe "articulos"

  }

//  it should "load config file when file is in hdfs" in {
//    val fakeHdfsContent =
//      """
//        |app.name = "MiAplicacion"
//        |app.port = 8080
//        |""".stripMargin
//
//    val fakeBytes = "key = value".getBytes("UTF-8")
//
//    val fsMock = mock(classOf[FileSystem])
//    val fsDataInputStreamMock = mock(classOf[FSDataInputStream])
//    when(fsDataInputStreamMock.read(any[Array[Byte]]())).thenAnswer { invocation =>
//      val buffer = invocation.getArgument(0).asInstanceOf[Array[Byte]]
//      System.arraycopy(fakeBytes, 0, buffer, 0, fakeBytes.length)
//      fakeBytes.length
//}
//      when(fsMock.open(any(classOf[Path]))).thenReturn(fsDataInputStreamMock)
//    mockStatic(classOf[FileSystem]).when(() =>
//      FileSystem.get(new URI("hdfs://ruta/fake.conf"), org.mockito.ArgumentMatchers.any(classOf[Configuration]))
//    ).thenReturn(fsMock)
//    val config: Config = ReadConfig.load("hdfs://ruta/fake.conf")
//
//    assert(config.hasPath("app.name"))
//    assert(config.getString("app.name") == "MiAplicacion")
//    assert(config.getInt("app.port") == 8080)
//
//
//
//  }

  it should "load: throw an exception for incorrect required path" in {

    an[Exception] should be thrownBy {
      ReadConfig.load("incorrect/path").getString("incorrect.value")
    }
  }

  it should "getOptionableConfig: get existing optionable config" in {
    val conf = ConfigFactory.parseString("""my.key = "valor123"""")
    val result = ReadConfig.getOptionableConfig(conf, "my.key")
    result shouldBe Some("valor123")
  }

  it should "getOptionableConfig: get None if config path does not exist" in {
    val conf = ConfigFactory.parseString("""otro.key = "valor" """)
    val result = ReadConfig.getOptionableConfig(conf, "unexisting.key")
    result shouldBe None
  }

  it should "getOptionableConfig: throw an exception if getString fails" in {
    val confMock = mock(classOf[Config])
    when(confMock.hasPath("bad.key")).thenReturn(true)
    when(confMock.getString("bad.key")).thenThrow(new RuntimeException("dummy fail"))

    val ex = intercept[RuntimeException] {
      ReadConfig.getOptionableConfig(confMock, "bad.key")
    }
    ex.getMessage should include ("dummy fail")
  }

//  it should  "whatever" in {
//    val confMock = mock(classOf[Config])
//    when(confMock.hasPath("bad.key")).thenReturn(true)
//    when(confMock.getString("bad.key")).thenReturn("valor")
//
//
//    val result = ReadConfig.getOptionableConfig(confMock, "bad.key")
//    result.shouldBe(Some("valor"))
//  }
  }
