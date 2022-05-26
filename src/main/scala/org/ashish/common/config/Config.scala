package org.ashish.common.config

import com.typesafe.config.ConfigFactory

class Config {

  def getKafkaProperties = {
    ConfigFactory.parseFile(new java.io.File("KAFKA/kafka.conf"))
  }

  def getPubSubProperties = {
    ConfigFactory.parseFile(new java.io.File("PUBSUB/pubsub.conf"))

  }
}