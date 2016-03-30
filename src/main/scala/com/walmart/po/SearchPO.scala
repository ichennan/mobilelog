package com.walmart.po

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.beans.BeanProperty
import org.apache.spark.sql.Row

/**
  * Created by Terry on 2016/3/29.
  */
class SearchPO {
  @BeanProperty var deviceId: String = _
  @BeanProperty var time: String = _
  @BeanProperty var screenSize: String = _
  @BeanProperty var nt: String = _
  @BeanProperty var ln: String = _
  @BeanProperty var model: String = _
  @BeanProperty var op: String = _
  @BeanProperty var systemName: String = _
  @BeanProperty var osVersion: String = _
  @BeanProperty var tz: String = _
  @BeanProperty var buildVersion: String = _
  @BeanProperty var name: String = _
  @BeanProperty var store: String = _
  @BeanProperty var keyword: String = _
  @BeanProperty var category: String = _
  @BeanProperty var resultCount: String = _

  def toRow():Row = {Row.apply(deviceId, time, screenSize, nt, ln, model, op, systemName, osVersion, tz, buildVersion, name, store, keyword, category, resultCount)}
}

object SearchPO {
  val structType = {StructType(Array[String]("deviceId", "time", "screenSize", "nt", "ln", "model", "op", "systemName", "osVersion", "tz", "buildVersion", "name", "store", "keyword", "category", "resultCount").map(fieldName => StructField(fieldName, StringType, true)))}
  val tableName = "table_search_terry"
  val creationString = "CREATE TABLE IF NOT EXISTS " + tableName +  "(deviceId STRING , time STRING ,screenSize STRING , nt STRING , ln STRING ,model STRING ,op STRING , systemName STRING ,osVersion STRING ,tz STRING ,buildVersion STRING ,name STRING,store STRING,keyword STRING,category STRING, resultCount STRING)"

  def saveRowRDD(rowRDD: List[Row], hiveContext: HiveContext, sc: SparkContext):Unit ={
    if(rowRDD.size > 0) hiveContext.createDataFrame(sc.parallelize(rowRDD), structType).insertInto(tableName)
  }
}
