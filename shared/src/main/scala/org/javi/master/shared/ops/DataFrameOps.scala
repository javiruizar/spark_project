package org.javi.master.shared.ops

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array_intersect, col, concat, concat_ws, lit, max, size, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object DataFrameOps {
  implicit class CustomDataFrameTransformations(df: DataFrame) {

    private[ops] def getFieldsOfNestedColumn (nestedColumnName: String): Array[StructField] = {
      df.select(nestedColumnName).schema.fields.head.dataType.asInstanceOf[StructType].fields
    }

    private[ops] def buildKeyValueColumn(nestedFields: Array[StructField], fatherColumnName: String): Array[Column] = {
      nestedFields.map { field =>
        val colName = field.name
        val colValue = col(s"$fatherColumnName.$colName")
        when(colValue.isNotNull, concat_ws(", ", concat_ws(":", lit(colName), colValue)))
      }
    }

    def concatenateNestedFieldsWithValues(nestedColumnName: String): Array[Column] ={
      val nestedFields = this.getFieldsOfNestedColumn(nestedColumnName)
      this.buildKeyValueColumn(nestedFields, nestedColumnName)
    }
    def getAllArticlesWithFeaturesDf(features: Array[Column]): DataFrame = {
      df.withColumn("caracteristicas", concat_ws(", ", features: _*))
        .withColumn("infoTotalArticulo", concat(lit("Articulo: "), col("nombre_articulo"), lit("\nCaracteristicas: \n"), col("caracteristicas")))
        .select("nombre_articulo", "palabras_clave", "infoTotalArticulo")
    }

    def getFoundArticlesDf(busqueda: Array[String]): DataFrame = {
      df
        .withColumn("busqueda", lit(busqueda))
        .withColumn("inters_size", size(array_intersect(col("busqueda"), col("palabras_clave"))))
        .filter(col("inters_size") > 0)
        .withColumn("max_value", max("inters_size").over())
        .filter(col("max_value") === col("inters_size"))
        .select(col("infoTotalArticulo").cast(StringType).as("value"))
    }
  }
}
