package org.ashish.common.config

import com.typesafe.config
import com.typesafe.config.ConfigFactory

class  Config {

  def getKafkaProperties = {
    val configFile = new java.io.File("src/main/resources/KAFKA/kafka.properties")
    val kafkaConfig = ConfigFactory.parseFile(configFile)
    kafkaConfig
  }

  def getPubSubProperties = {
    val configFile = new java.io.File("src/main/resources/PUBSUB/pubsub.properties")
    val pubsubConfig = ConfigFactory.parseFile(configFile)
    pubsubConfig
  }
}