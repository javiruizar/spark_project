  package org.javi.master.streaming.processing

  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
  import org.scalatest.BeforeAndAfterAll
  import org.scalatest.flatspec.AnyFlatSpec
  import org.scalatest.matchers.should.Matchers
  class QueryProcessorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

    private var spark: SparkSession = _

    override def beforeAll(): Unit = {
      spark = SparkSession.builder()
        .appName("QueryProcessorTest")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    }

    override def afterAll(): Unit = {
      if (spark != null) {
        spark.stop()
      }
    }

    "QueryProcessor.process" should "filter articles based on search keywords" in {
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val schema = StructType(Seq(
        StructField("id", StringType),
        StructField("palabras_clave", ArrayType(StringType)),
        StructField("valores", StringType)
      ))

      val testData = Seq(
        ("1", Array("tecnologia", "movil"), "Valor1"),
        ("2", Array("deportes", "futbol"), "Valor2"),
        ("3", Array("tecnologia", "ordenador"), "Valor3"),
        ("4", Array("tecnologia", "movil", "smartphone"), "Valor4")
      )

      val articles = spark.createDataFrame(testData)
        .toDF("id", "palabras_clave", "valores")

      // Test with search terms that should match
      val searchTerms = Array("tecnologia", "movil")
      val result = QueryProcessor.process(articles, searchTerms)

      // Verify results
      val results = result.collect()
      results should have length 2
      results.map(_.getString(0)).toSet shouldBe Set("Valor1", "Valor4")
    }

    it should "return empty result when no matches found" in {
      val spark = this.spark


      import spark.implicits._

      val testData = Seq(
        ("1", Array("deportes", "futbol"), "Valor1"),
        ("2", Array("cocina", "recetas"), "Valor2")
      )

      val articles = spark.createDataFrame(testData)
        .toDF("id", "palabras_clave", "valores")

      val searchTerms = Array("tecnologia", "programacion")
      val result = QueryProcessor.process(articles, searchTerms)

      result.count() shouldBe 0
    }

    it should "return articles with highest match count when multiple matches exist" in {
      val spark = this.spark

      import spark.implicits._

      val testData = Seq(
        ("1", Array("tecnologia"), "Valor1"),
        ("2", Array("tecnologia", "movil"), "Valor2"),
        ("3", Array("tecnologia", "movil", "smartphone"), "Valor3")
      )

      val articles = spark.createDataFrame(testData)
        .toDF("id", "palabras_clave", "valores")

      val searchTerms = Array("tecnologia", "movil", "smartphone")
      val result = QueryProcessor.process(articles, searchTerms)

      // Only the article with all 3 matching terms should be returned
      val results = result.collect()
      results should have length 1
      results.head.getString(0) shouldBe "Valor3"
    }

    it should "return multiple articles when they have the same highest match count" in {
      val spark = this.spark
      import spark.implicits._

      val testData = Seq(
        ("1", Array("tecnologia", "movil"), "Valor1"),
        ("2", Array("tecnologia", "movil"), "Valor2"),
        ("3", Array("tecnologia"), "Valor3")
      )

      val articles = spark.createDataFrame(testData)
        .toDF("id", "palabras_clave", "valores")

      val searchTerms = Array("tecnologia", "movil")
      val result = QueryProcessor.process(articles, searchTerms)

      // Both articles with 2 matching terms should be returned
      val results = result.collect()
      results should have length 2
      results.map(_.getString(0)).toSet shouldBe Set("Valor1", "Valor2")
    }
  }
