package org.ashish.kafka.config
import java.util.Properties
import org.apache.log4j.PropertyConfigurator

class Config {

  def readProperties(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("src/main/resources/development.properties"))
    PropertyConfigurator.configure(properties)
  }
}
