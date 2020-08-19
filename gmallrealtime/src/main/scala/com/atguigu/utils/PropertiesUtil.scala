package com.atguigu.utils
import java.io.InputStreamReader
import java.util.Properties
/**
 * Author: doubleZ
 * Datetime:2020/8/15   21:02
 * Description:
 */
object PropertiesUtil{
  def load(perprotieName:String): Properties ={
    val properties: Properties = new Properties()
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(perprotieName),"UTF-8"))
    properties
  }
}

object PropertiesUtil2{
  def load(propertiesName:String)={
    val properties: Properties = new Properties()
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),"UTF-8"))
    properties
  }

}