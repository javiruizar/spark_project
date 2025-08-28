package org.javi.master.shared.utils.io

import com.typesafe.config.{Config, ConfigFactory}
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.transitions.{Mongod, RunningMongodProcess}
import de.flapdoodle.reverse.transitions.Start
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class MongoWriterTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // --- Variables para la BD embebida y Spark ---
  // 1. Iniciar MongoDB en un puerto aleatorio
  private lazy val mongod: RunningMongodProcess = Mongod.builder()
    .net(Start.to(classOf[Net]).initializedWith(Net.defaults()))
    .build()
    .start(Version.V6_0_4).asState().value()

  private val port: Int = mongod.getServerAddress.getPort
  private val testDb: String = "testdb"
  private val testCollection: String = "testCollection"
  private val mongoUri: String = s"mongodb://localhost:$port"

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("MongoWriterTest")
    .config("spark.driver.host", "localhost") // Evita problemas de red en algunos sistemas
    .config("spark.ui.enabled", "false")
    .config("spark.mongodb.output.uri", s"$mongoUri/$testDb.$testCollection")
    .getOrCreate()

  val schema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true)
  ))
  val data: Seq[Row] = Seq(Row(1, "test1"), Row(2, "test2"))
  val testDf: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)


  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (mongod != null) mongod.stop()
    super.afterAll()
  }

  "MongoWriter.write" should "write a DataFrame to MongoDB correctly" in {

//    val testConfig = ReadConfig.load("shared/src/test/resources/conf/batch.conf")

    val testConfig = ConfigFactory.parseString(
      f"""
        |spark {
        |   mongodb {
        |        output {
        |         uri = "$mongoUri"
        |         database = "$testDb"
        |         collection = "$testCollection"
        |            }
        |        }
        |    }
        |""".stripMargin)

    MongoWriter.write(testDf, testConfig)

    val writtenDf = spark.read
      .format("mongodb")
      .option("connection.uri", testConfig.getString("spark.mongodb.output.uri"))
      .option("database", testConfig.getString("spark.mongodb.output.database"))
      .option("collection", testConfig.getString("spark.mongodb.output.collection"))
      .load()
      // Seleccionamos y ordenamos para una comparación consistente
      .select("id", "name")
      .orderBy("id")


    val originalData = testDf.collectAsList().asScala
    val writtenData = writtenDf.collectAsList().asScala

    writtenDf.count() shouldBe 2

    writtenData should contain theSameElementsAs originalData
  }

  "MongoWriter.write" should "Throw an exception when parameters are incorrect" in {

    val testConfigWrongUri = ConfigFactory.parseString(
      f"""
         |spark {
         |   mongodb {
         |        output {
         |         uri = "WRONG-MONGO-URI"
         |         database = "$testDb"
         |         collection = "$testCollection"
         |            }
         |        }
         |    }
         |""".stripMargin)

    an[Exception] should be thrownBy {
      MongoWriter.write(testDf, testConfigWrongUri)
    }
  }
}