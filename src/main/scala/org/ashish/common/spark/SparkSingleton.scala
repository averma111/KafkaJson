package org.ashish.common.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.ashish.common.config.Config
import org.ashish.impl.Logging


import scala.sys.exit

class SparkSingleton extends Logging  {

  private val pubsubConfig = new Config
  def createSparkStreamingContext: StreamingContext ={
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(pubsubConfig.getPubSubProperties.getString("APP_NAME"))
      .setMaster("local[*]")
    if(sparkConf == null) {
      logger.warn("Failed to create the spark conf.Exiting the process")
      exit(1)
    }
    val ssc = new StreamingContext(sparkConf, Milliseconds(10000))
    if(ssc == null) {
      logger.warn("Failed to create the streaming context.Exiting the process")
      exit(1)
    }
    ssc
  }

  def getInstance(sparkConf: SparkConf): SparkSession = {
    val sparkInstance = SparkSession.builder().config(sparkConf).getOrCreate()
    if (sparkInstance == null) {
      logger.warn("Failed to create the sparkInstance.Exiting the process")
      exit(1)
    }
    sparkInstance
  }

}
