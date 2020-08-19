package com.atguigu.bean

/**
 * Author: doubleZ
 * Datetime:2020/8/15   21:05
 * Description:
 */
case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      `type`:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long)
