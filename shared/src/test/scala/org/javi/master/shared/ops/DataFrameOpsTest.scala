package org.javi.master.shared.ops

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.{be, contain, include}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.DataFrame

import org.javi.master.shared.ops.DataFrameOps._

class DataFrameOpsTest extends AnyFlatSpec
  with BeforeAndAfterAll
  with DataFrameComparer
  with TestData {

  val spark: SparkSession = SparkSession.builder()
    .appName("test-app")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  val structDataFrame: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(dataStruct), schemaStruct)


  "getFieldsOfNestedColumn" should "return array of columns in struct field" in {
    val featureColumns = structDataFrame.getFieldsOfNestedColumn("caracteristicas_venta")
    val expectedFields = Array("color", "talla", "modelo")

    featureColumns.map(_.name) should contain theSameElementsAs expectedFields
  }

  "getFieldsOfNestedColumn" should "throw exception when field is not nested" in {

    an[Exception] should be thrownBy {
      structDataFrame.getFieldsOfNestedColumn("id")
    }
  }

  "getFieldsOfNestedColumn" should "throw exception when field does not exist" in {
    an[Exception] should be thrownBy {
      structDataFrame.getFieldsOfNestedColumn("BAD_Field")
    }
  }

  "buildKeyValueColumns" should "build an array of columns with the corresponding value from nested field" in {

    val nestedField = structDataFrame.select("caracteristicas_venta").schema
      .fields.head.dataType.asInstanceOf[StructType].fields

    val result = structDataFrame.buildKeyValueColumn(nestedField, "caracteristicas_venta")
    val expectedFields = Array("color", "talla", "modelo")

    result.length shouldBe expectedFields.length

    result.zip(expectedFields).foreach { case (colExpr, expectedName) =>
      val exprStr = colExpr.expr.sql.toLowerCase
      //      withClue(s"Error en columna esperada '$expectedName' -> expresión: $exprStr") {
      exprStr should include(s"caracteristicas_venta.$expectedName")
      //      }
    }
  }

  "concatenateFieldsOfNestedColumn" should "work" in {
    val result = structDataFrame.concatenateNestedFieldsWithValues("caracteristicas_venta")
    val expectedFields = Array("color", "talla", "modelo")
    result.length shouldBe expectedFields.length
    result.zip(expectedFields).foreach { case (colExpr, expectedName) =>
      val exprStr = colExpr.expr.sql.toLowerCase // forma SQL interna de la columna
      exprStr should include(s"caracteristicas_venta.$expectedName")
    }
  }

  "getAllArticlesWithFeaturesDf" should "get selected fields with features concatenated in String field" in {
    val features = Array(col("caracteristicas_venta.color"), col("caracteristicas_venta.talla"), col("caracteristicas_venta.modelo"))
    val result = structDataFrame.getAllArticlesWithFeaturesDf(features)
    val expectedSchema = StructType(Seq(
      StructField("nombre_articulo", StringType),
      StructField("palabras_clave", ArrayType(StringType)),
      StructField("infoTotalArticulo", StringType)
    ))

    val expectedData = Seq(
      Row("articulo_1", Array("rojo"), "Articulo: articulo_1\nCaracteristicas: \nrojo, A, modeloA"),
      Row("articulo_2", Array("azul"), "Articulo: articulo_2\nCaracteristicas: \nazul, modeloB")
    )

    val expectedDataFrame: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), expectedSchema)
    assertSmallDataFrameEquality(result, expectedDataFrame)
  }

  "getFoundArticlesDf" should "build dataframe with only matched articles" in {
    import spark.implicits._
    val previousData = structDataFrame
      .withColumn("infoTotalArticulo",
        when(
          col("nombre_articulo") === lit("articulo_1"),
          lit("Articulo: articulo_1\nCaracteristicas: \nrojo, A, modeloA"))
          when(
          col("nombre_articulo") === lit("articulo_2"),
          lit("Articulo: articulo_2\nCaracteristicas: \nazul, modeloB")
        ))

    val result = previousData.getFoundArticlesDf(Array("rojo"))
    val expected = Seq("Articulo: articulo_1\nCaracteristicas: \nrojo, A, modeloA").toDF("value")

    assertSmallDataFrameEquality(result, expected)
  }
}