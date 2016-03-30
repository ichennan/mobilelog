package com.walmart.po

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.beans.BeanProperty

/**
  * Created by Terry on 2016/3/29.
  */
class SearchFilePO {
  @BeanProperty var filePath: String = _

  def toRow(): Row = {
    Row.apply(filePath)
  }
}

object SearchFilePO {
  val structType = {
    StructType(Array[String]("filePath").map(fieldName => StructField(fieldName, StringType, true)))
  }
  val tableName = "table_file_search_terry"
  val creationString = "CREATE TABLE IF NOT EXISTS " + tableName + "(filePath STRING)"

  def saveRowRDD(rowRDD: List[Row], hiveContext: HiveContext, sc: SparkContext): Unit = {
    if (rowRDD.size > 0) hiveContext.createDataFrame(sc.parallelize(rowRDD), structType).insertInto(tableName)
  }
}
