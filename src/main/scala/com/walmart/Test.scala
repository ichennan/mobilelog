package com.walmart

import com.walmart.po.SearchPO
import com.walmart.util.ConfigProperties

/**
  * Created by Terry on 2016/3/29.
  */
object Test {
  def main(args: Array[String]){
    println(ConfigProperties.getString("LOG_PATH"))
    println(ConfigProperties.getString("LOG_PATH_SEARCH"))
    println(ConfigProperties.getString("test3333"))
    println("i am testing")
    println(SearchPO.tableName)
  }
}
