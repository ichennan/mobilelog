package com.walmart

import java.util.ArrayList

import com.walmart.po.{SearchFilePO, SearchPO}
import com.walmart.util.{ConfigProperties, JsonUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

/**
  * Created by Terry on 2016/3/29.
  */
object SearchLogParse {
  var searchPoList = List[Row]()
  var newFileList = List[String]()

  def main(args: Array[String]) {
    // logger
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //environment setup
    val systime = System.currentTimeMillis()
    println("!!!SearchLogParse!!!")
    val conf = new SparkConf().setAppName("SearchLogParse")
    println("!!!conf is " + conf)
    val sc = new SparkContext(conf)
    println("!!!sc is " + sc)
    val hiveContext = new HiveContext(sc)
    println("!!!new HiveContext!!!")
    val pathString = {
      if (args.length == 1) args(0)
      else ConfigProperties.getString("LOG_PATH_SEARCH")
    }
    val filePath = new Path(pathString)
    println("!!!filePath is " + filePath)
    val fs = FileSystem.get(new Configuration())

    //hive table checking
    hiveContext.sql("use mobile")
    hiveContext.sql(SearchPO.creationString)
    hiveContext.sql(SearchFilePO.creationString)
    val fileList = new ArrayList[String]()
    hiveContext.sql("select * from " + SearchFilePO.tableName).collect().foreach { x => fileList.add(x.getString(0)) }
    val fileListSize = fileList.size()

    //deal with the file in filePath
    doFile(sc, hiveContext, fs, filePath, fileList)
    SearchPO.saveRowRDD(searchPoList, hiveContext, sc)
    SearchFilePO.saveRowRDD(newFileList.map {
      Row(_)
    }, hiveContext, sc)

    println("Finished!!!")
    sc.stop()
    println("==========(" + (fileList.size - fileListSize) + " files) GetSearchData success time is " + (System.currentTimeMillis() - systime))
  }

  def doFile(sc: SparkContext, hiveContext: HiveContext, fs: FileSystem, filePath: Path, fileList: ArrayList[String]) {
    println("==========doFile filePath is " + filePath)
    try {
      val fileState = fs.listStatus(filePath)
      fileState.foreach { status =>
        if (!status.isDirectory()) {
          val path = status.getPath.toString()
          val name = status.getPath.getName
          //println("==========doFile path is :" + path)
          if (name.split("_").length == 2) {
            if (name.length > 10 && name.substring(name.length() - 10, name.length()).compareTo("2015-07-31") >= 0) {
              //println("==========doFile name is :" + name + ";date is :" + name.substring(name.length() -10, name.length()))
              if (!fileList.contains(path)) {
                searchPoList = searchPoList ::: insertData(sc, hiveContext, path)
                newFileList = newFileList.::(path)
                fileList.add(path)
                //Avoid size exceed
                if (newFileList.size >= 100) {
                  SearchPO.saveRowRDD(searchPoList, hiveContext, sc)
                  SearchFilePO.saveRowRDD(newFileList.map {
                    Row(_)
                  }, hiveContext, sc)
                  searchPoList = List[Row]()
                  newFileList = List[String]()
                }
              }
            }
          }
        } else {
          println("==========doFile dir is " + status.getPath())
          doFile(sc, hiveContext, fs, status.getPath(), fileList)
        }
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace() // TODO: handle error
        println("************insertData failed path is " + filePath)
    }
  }

  def insertData(sc: SparkContext, hiveContext: HiveContext, path: String): List[Row] = {
    println("==========path: " + path)
    var searchPoList_file = List[Row]()
    var linesInFile = 0
    var searchPo = new SearchPO
    sc.textFile(path).collect().foreach { line =>
      if (line.indexOf("{") > -1) {
        if (searchPo != null && searchPo.time != null) searchPoList_file = searchPoList_file.::(searchPo.toRow())
        searchPo = new SearchPO
      }
      dealLine(line, searchPo)
      linesInFile = linesInFile + 1
    }
    println("==========lines: " + linesInFile + ", items: " + searchPoList_file.size)
    searchPoList_file
  }

  def dealLine(line: String, searchPo: SearchPO) = {
    line match {
      case _ if (line.indexOf("{") > -1) => {
        try {
          searchPo.time = line.substring(1, 20)
          val clientInfoObj = new JSONObject(line.substring(line.indexOf("{"), line.indexOf("}") + 1))
          if (clientInfoObj != null) {
            searchPo.deviceId = JsonUtils.getString(clientInfoObj, "deviceId")
            searchPo.screenSize = JsonUtils.getString(clientInfoObj, "screenSize")
            searchPo.nt = JsonUtils.getString(clientInfoObj, "nt")
            searchPo.ln = JsonUtils.getString(clientInfoObj, "ln")
            searchPo.model = JsonUtils.getString(clientInfoObj, "model")
            searchPo.op = JsonUtils.getString(clientInfoObj, "op")
            searchPo.systemName = JsonUtils.getString(clientInfoObj, "systemName")
            searchPo.osVersion = JsonUtils.getString(clientInfoObj, "OsVersion")
            searchPo.tz = JsonUtils.getString(clientInfoObj, "tz")
            searchPo.buildVersion = JsonUtils.getString(clientInfoObj, "buildVersion")
            searchPo.name = JsonUtils.getString(clientInfoObj, "name")
          }
        } catch {
          case t: Throwable => printErrorLine(line)
        }
      }
      case _ if (line.indexOf("user search info:") > -1) => {
        if (searchPo == null) printErrorLine(line)
        val keywordStr = line.substring(line.indexOf("user search info") + 17, line.length).split(",")
        searchPo.store = keywordStr(0)
        if (keywordStr.length == 3) searchPo.category = keywordStr(2)
        else if (keywordStr.length == 2) searchPo.keyword = keywordStr(1)
      }
      case _ if (line.indexOf("The results size is:") > -1) => {
        if (searchPo == null) printErrorLine(line)
        val keywordStr = line.substring(line.indexOf("The results size is:") + 20, line.length).split(",")
        if (keywordStr.length == 1) searchPo.resultCount = keywordStr(0)
        else printErrorLine(line)
      }
      case _ => {
        printErrorLine(line)
      }
    }
  }

  def printErrorLine(errorLine: String): Unit = {
    println("**********error line:" + errorLine)
  }
}
