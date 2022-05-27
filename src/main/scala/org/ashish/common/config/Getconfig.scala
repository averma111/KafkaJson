package org.ashish.common.config

import com.typesafe.config.ConfigFactory

class Getconfig {

  def getKafkaProperties = {
    val kafkaConfig = ConfigFactory.load("KAFKA/kafka.properties")
    kafkaConfig
  }

  def getPubSubProperties = {
   val pubsubConfig= ConfigFactory.load("PUBSUB/pubsub.properties")
   pubsubConfig

  }
}