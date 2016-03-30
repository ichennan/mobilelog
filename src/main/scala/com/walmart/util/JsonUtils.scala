package com.walmart.util

import org.json.JSONObject
/**
  * Created by Terry on 2016/3/29.
  */
class JsonUtils {

}

object JsonUtils {
  def getString(json: JSONObject, key: String):String = {
    if(json.has(key))
      json.getString(key)
    else
      ""
  }
}
