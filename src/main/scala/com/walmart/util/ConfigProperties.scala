package com.walmart.util

import java.util

import scala.io.Source
/**
  * Created by Terry on 2016/3/29.
  */
class ConfigProperties {
  val properties = new util.HashMap[String, String]()
  def load(filePath:String):Unit = {
    val source =  Source.fromInputStream(ConfigProperties.getClass.getClassLoader.getResourceAsStream(filePath))
    source.getLines.filterNot(_.startsWith("#")).map(_.split("=")).foreach(addToMap)
  }

  def addToMap(map: Array[String]): Unit = {
    map.length match{
      case 2 => properties.put(map(0).trim, map(1).trim)
      case _ =>
    }
  }

  def getString(key: String): String ={
    Option(properties.get(key)).getOrElse("")
  }
}

object ConfigProperties{
  val configProperties = new ConfigProperties
  configProperties.load("config.properties")

  def getString(key: String): String ={
    configProperties getString(key)
  }
}
