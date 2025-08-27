//package org.javi.master.streaming
//
//import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
//import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
//import org.javi.master.streaming.config.StreamingConfig
//import org.javi.master.streaming.processing.QueryProcessor
//import org.mockito.ArgumentMatchers.anyString
//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.BeforeAndAfterAll
//import org.scalatestplus.mockito.MockitoSugar
//import org.mockito.Mockito._
//
//class StreamingAppTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
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
//    spark = SparkSession.builder()
//      .appName("StreamingAppTest")
//      .master("local[2]")
//      .config("spark.ui.enabled", "false")
//      .getOrCreate()
//  }
//
//  override def afterAll(): Unit = {
//    if (spark != null) {
//      spark.stop()
//    }
//  }
//
//  "StreamingApp" should "process articles and prepare them for Kafka" in {
//    val spark = this.spark
//    import spark.implicits._
//
//    // Create test data that matches the expected MongoDB schema
//    val testData = Seq(
//      ("Art1", Array("tecnologia", "movil"), Map("marca" -> "Samsung", "modelo" -> "Galaxy S21")),
//      ("Art2", Array("deportes", "futbol"), Map("marca" -> "Nike", "talla" -> "M")),
//      ("Art3", Array("tecnologia", "ordenador"), Map("marca" -> "Dell", "procesador" -> "i7"))
//    )
//
//    // Create a DataFrame that matches the expected structure from MongoDB
//    val articlesDF = testData.toDF("nombre_articulo", "palabras_clave", "caracteristicas_venta")
//
//    // Mock the MongoDB read
//    val mockSpark = mock[SparkSession]
//    val mockRead = mock[DataFrame]
//
////    when(mockSpark.read) thenReturn mock[org.apache.spark.sql.DataFrameReader]
////    when(mockSpark.read.format(anyString)) thenReturn mock[org.apache.spark.sql.DataFrameReader]
////    when(mockSpark.read.format(anyString).load()) thenReturn articlesDF
//
//    // Test the article processing logic
//    import org.javi.master.streaming.StreamingApp._
//
//    // Since we can't easily test the streaming part, we'll test the DataFrame transformations
//    // that would be applied to the streaming data
//    val processedDF = processArticles(articlesDF, Array("tecnologia"))
//
//    // Verify the results
//    val results = processedDF.collect()
//    results should have length 2  // Should match 2 technology articles
//
//    // Verify the output format
//    results.foreach { row =>
//      val value = row.getString(0)
//      value should startWith ("Articulo: Art")
//      value should include ("Caracteristicas:")
//    }
//  }
//
//  // Helper method to test the article processing logic
//  private def processArticles(articlesDF: DataFrame, searchTerms: Array[String]): DataFrame = {
//    import org.apache.spark.sql.functions._
//
//    val spark = this.spark
//    import spark.implicits._
//
//    val caracteristicas_venta = articlesDF.select("caracteristicas_venta").schema.fields.head.dataType.asInstanceOf[StructType].fields
//
//    val keyValueColumns = caracteristicas_venta.map { field =>
//      val colName = field.name
//      val colValue = col(s"caracteristicas_venta.$colName")
//      when(colValue.isNotNull, concat_ws(", ", concat_ws(":", lit(colName), colValue)))
//    }
//
//    val processedDF = articlesDF
//      .withColumn("clave_valor", concat_ws(", ", keyValueColumns: _*))
//      .withColumn("valores", concat(lit("Articulo: "), $"nombre_articulo", lit("\nCaracteristicas: \n"), $"clave_valor"))
//      .select("nombre_articulo", "palabras_clave", "valores")
//
//    // Apply the query processing
//    QueryProcessor.process(processedDF, searchTerms)
//  }
//
//  it should "use local Kafka bootstrap server in local mode" in {
//    // This would test the bootstrap server selection logic
//    // Implementation would be similar to above but checking the bootstrap server selection
//    pending
//  }
//
//  it should "use cluster Kafka bootstrap server in cluster mode" in {
//    // This would test the bootstrap server selection logic
//    pending
//  }
//}
