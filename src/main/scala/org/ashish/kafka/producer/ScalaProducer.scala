package org.ashish.kafka.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.ashish.common.config.Config
import org.ashish.impl.Logging

import java.util.{Calendar, Properties, UUID}
import org.ashish.kafka.dto.FastMessage
import org.ashish.kafka.dto.FastMessageJsonImplicits._
import play.api.libs.json.Json

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.math.abs
import scala.sys.exit
import scala.util.Random

class ScalaProducer extends Logging {
  private val kafkaConfig = new Config
  def send(args: Array[String], topic: String): Unit = {

    logger.warn("=============Starting the producer job================")
    var producer: KafkaProducer[String, String] = null
    val properties = new Properties()
    properties.put("bootstrap.servers", kafkaConfig.getKafkaProperties.getString("BOOTSTRAP_SERVERS"))
    properties.put("key.serializer", kafkaConfig.getKafkaProperties.getString("KEY_SERIALIZER"))
    properties.put("value.serializer", kafkaConfig.getKafkaProperties.getString("VALUE_SERIALIZER"))
    properties.put("auto.commit.interval.ms", kafkaConfig.getKafkaProperties.getString("INTERVAL_MS"))
    properties.put("max.block.ms", kafkaConfig.getKafkaProperties.getString("MAX_BLOCK_MS"))
    producer = new KafkaProducer[String, String](properties)

    if (producer == null) {
      logger.warn("Failed to created the producer instance")
      exit(1)
    }
    try {
      var jsonText: String = ""
      while (true) {
        jsonText = Json.toJson(
          FastMessage("FastMessage_" + Calendar.getInstance().getTimeInMillis.toString,
            UUID.randomUUID().toString,
            DateTimeFormatter.ofPattern(kafkaConfig.getKafkaProperties.getString("TIMESTAMP_PATTERN")).format(LocalDateTime.now())
          )
        ).toString()
        logger.warn(s"The json to be pushed to topic:->${jsonText}")
        producer.send(new ProducerRecord[String, String](topic,
          abs(Random.nextInt()).toString,
          jsonText))
        producer.flush()
        logger.warn("Data sent to topic")
        Thread.sleep(kafkaConfig.getKafkaProperties.getString("TIME_MILLI_SEC").toLong)
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