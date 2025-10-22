package org.javi.master.shared.utils.mongo

import com.typesafe.config.ConfigFactory
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.transitions.{Mongod, RunningMongodProcess}
import de.flapdoodle.reverse.transitions.Start
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.javi.master.shared.utils.mongo.MongoUtils.getMongoConfig
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class MongoUtilsTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll
  with BeforeAndAfterEach{

  // --- Variables para la BD embebida y Spark ---
  private var mongod: RunningMongodProcess = _
  private var spark: SparkSession = _
  private var testDf: DataFrame = _
  private var mongoUri: String = _
  private val testDb: String = "testdb"
  private val testCollection: String = "testCollection"

  // 1. MUEVE TODA LA LÓGICA DE CREACIÓN A beforeAll
  override def beforeAll(): Unit = {
    super.beforeAll()

    // Iniciar MongoDB embebido
    mongod = Mongod.builder()
      .net(Start.to(classOf[Net]).initializedWith(Net.defaults()))
      .build()
      .start(Version.V6_0_4).asState().value()
    val port = mongod.getServerAddress.getPort
    mongoUri = s"mongodb://localhost:$port"

    // Crear SparkSession aquí
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoUtilsTest")
      .config("spark.driver.host", "localhost")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Crear el DataFrame de prueba aquí, una vez que Spark existe
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    val data = Seq(Row(1, "test1"), Row(2, "test2"))
    testDf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  // 2. afterAll ya estaba bien, se encarga de limpiar
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (mongod != null) {
      mongod.stop()
    }
    super.afterAll()
  }
  "MongoUtils.writeMongo" should "writeMongo a DataFrame to MongoDB correctly" in {

//    val testConfig = ReadConfig.load("shared/src/test/resources/conf/batch.conf")

    val testConfig = ConfigFactory.parseString(
      f"""
        |   mongodb {
        |        output {
        |         uri = "$mongoUri"
        |         database = "$testDb"
        |         collection = "$testCollection"
        |            }
        |        }
        |""".stripMargin)

    val mongoconfig:MongoConfig= getMongoConfig(testConfig)

    MongoUtils.writeMongo(testDf, mongoconfig)

    val writtenDf = spark.read
      .format("mongodb")
      .option("connection.uri", testConfig.getString("mongodb.output.uri"))
      .option("database", testConfig.getString("mongodb.output.database"))
      .option("collection", testConfig.getString("mongodb.output.collection"))
      .load()
      // Seleccionamos y ordenamos para una comparación consistente
      .select("id", "name")
      .orderBy("id")


    val originalData = testDf.collectAsList().asScala
    val writtenData = writtenDf.collectAsList().asScala

    writtenDf.count() shouldBe 2

    writtenData should contain theSameElementsAs originalData
  }

  "MongoUtils.writeMongo" should "Throw an exception when parameters are incorrect" in {

    val testConfigWrongUri = ConfigFactory.parseString(
      f"""
         |   mongodb {
         |        output {
         |         uri = "WRONG-MONGO-URI"
         |         database = "$testDb"
         |         collection = "$testCollection"
         |            }
         |        }
         |""".stripMargin)

    val mongoconfig:MongoConfig= getMongoConfig(testConfigWrongUri)
    an[Exception] should be thrownBy {
      MongoUtils.writeMongo(testDf, mongoconfig)
    }
  }

  "MongoUtils.readMongo" should "Read mongo collection when parameters are correct" in {

    val testConfig = ConfigFactory.parseString(
      f"""
         |mongodb {
         |  input {
         |    uri = "$mongoUri"
         |    database = "$testDb"
         |    collection = "$testCollection"
         |  }
         |  output {
         |    uri = "$mongoUri"
         |    database = "$testDb"
         |    collection = "$testCollection"
         |  }
         |}
         |""".stripMargin)

    val mongoConfig = getMongoConfig(testConfig)
    MongoUtils.writeMongo(testDf, mongoConfig)

    val readDf = MongoUtils.readMongo(spark, mongoConfig)
      .select("id", "name")
      .orderBy("id")

    val readData = readDf.collectAsList().asScala
    val originalData = testDf.collectAsList().asScala

    readDf.count() shouldBe testDf.count()
    readData should contain theSameElementsAs originalData


  }

  "MongoUtils.readMongo" should "throw an exception when the config is invalid" in {

    val wrongConfig = ConfigFactory.parseString(
      f"""
         |mongodb {
         |  input {
         |    uri = "WRONG-MONGO-URI"
         |    database = "WRONG-DB"
         |    collection = "WRONG-COLLECTION"
         |  }
         |}
         |""".stripMargin)

    val mongoConfig = getMongoConfig(wrongConfig)

    an[Exception] should be thrownBy {
      MongoUtils.readMongo(spark, mongoConfig)
    }
  }
  override protected def beforeEach(): Unit = {
    // Limpia la colección antes de cada test
    val mongoClient = com.mongodb.client.MongoClients.create(mongoUri)
    try {
      val database = mongoClient.getDatabase(testDb)
      database.getCollection(testCollection).deleteMany(new org.bson.Document())
    } finally {
      mongoClient.close()
    }
    super.beforeEach()
  }
  }