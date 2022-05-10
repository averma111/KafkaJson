package org.ashish.kafka.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.{Calendar, Properties, UUID}
import com.google.common.io.Resources
import org.ashish.kafka.dto.FastMessage
import org.ashish.kafka.dto.FastMessageJsonImplicits._
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.math.abs
import scala.sys.exit
import scala.util.Random

object ScalaProducer {
  val topic = "fast-messages"

  def main(args: Array[String]): Unit = {
    val scalaProducer = new ScalaProducer()
    scalaProducer.send(args, topic)
  }
}

class ScalaProducer {

 lazy val LOGGER: Logger = LoggerFactory.getLogger(ScalaProducer.getClass)

  private def send(args: Array[String], topic: String): Unit = {
    LOGGER.warn("=============Starting the producer job================")
    var producer: KafkaProducer[String, String] = null
    val props = Resources.getResource("producer.properties").openStream()
    val properties = new Properties()
    properties.load(props)
    producer = new KafkaProducer[String, String](properties)
    if (producer == null) {
      LOGGER.warn("Failed to created the producer instance")
      exit(1)
    }
    try {
      var jsonText: String = ""
      while (true) {
        jsonText = Json.toJson(
          FastMessage("FastMessage_" + Calendar.getInstance().getTimeInMillis.toString,
            UUID.randomUUID().toString,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.ms").format(LocalDateTime.now())
          )
        ).toString()
        LOGGER.warn(s"The json to be pushed to topic:->${jsonText}")
        producer.send(new ProducerRecord[String, String](topic,
          abs(Random.nextInt()).toString,
          jsonText))
        producer.flush()
        LOGGER.warn("Data sent to topic")
        Thread.sleep(2000L)
      }
    }
    catch {
      case throwable: Throwable =>
        val st = throwable.getStackTrace
        println(s"Got exception : ${st.toString}")
    }
    finally {
      producer.close()
    }
  }
}