package org.ashish.kafka.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.{Calendar, Properties, UUID}
import com.google.common.io.Resources
import org.ashish.kafka.dto.FastMessage
import org.ashish.kafka.dto.FastMessageJsonImplicits._
import play.api.libs.json.Json

import scala.math.abs
import scala.util.Random

object ScalaProducer {
  def main(args: Array[String]): Unit = {
    val scalaProducer = new ScalaProducer()
    scalaProducer.send(args)
  }
}

class ScalaProducer {
  private def send(args: Array[String]): Unit = {
    println("Press enter to start producer")
    scala.io.StdIn.readLine()

    var producer: KafkaProducer[String, String] = null
    try {
      val props = Resources.getResource("producer.properties").openStream()
      val properties = new Properties()
      properties.load(props)
      producer = new KafkaProducer[String, String](properties)
      var jsonText: String = ""
      while (true) {
        jsonText= Json.toJson(
          FastMessage("FastMessage_"+Random.nextInt().toString+"_"+Calendar.getInstance().getTimeInMillis.toString,
                        UUID.randomUUID().toString)
        ).toString()
        producer.send(new ProducerRecord[String, String]("fast-messages",
          abs(Random.nextInt()).toString,
          jsonText))
        producer.flush()
        println("Sent msg")
      }
    } catch {

      case throwable: Throwable =>
        val st = throwable.getStackTrace
        println(s"Got exception : $st")
    }
    finally {
      producer.close()
    }
  }
}