package org.javi.master.shared.ops

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

trait TestData {
  val schemaStruct = StructType(Seq(
    StructField("id", IntegerType),
    StructField("palabras_clave", ArrayType(StringType)),
    StructField("caracteristicas_venta", StructType(Seq(
      StructField("color", StringType, nullable = true),
      StructField("talla", StringType, nullable = true),
      StructField("modelo", StringType,nullable = true)
    ))),
    StructField("nombre_articulo", StringType)
  ))

  val dataStruct = Seq(
    Row(1, Array("rojo"),Row("rojo", "A", "modeloA"),"articulo_1"),
    Row(2, Array("azul"),Row("azul", null, "modeloB"),"articulo_2")
  )

}