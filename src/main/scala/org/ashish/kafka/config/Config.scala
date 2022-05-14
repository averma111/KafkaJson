package org.ashish.kafka.config

import com.typesafe.config
import com.typesafe.config.ConfigFactory
class  Config {

  def getProperties(): config.Config = {
    val configFile = new java.io.File("src/main/resources/kafka.properties")
    val config = ConfigFactory.parseFile(configFile)
    config
  }
}