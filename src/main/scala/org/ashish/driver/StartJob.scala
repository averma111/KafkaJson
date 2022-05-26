package org.ashish.driver

import org.ashish.common.config.Config
import org.ashish.common.spark.SparkSingleton
import org.ashish.impl.Logging
import org.ashish.kafka.producer.ScalaProducer
import org.ashish.pubsub.consumer.PubSub
import scala.sys.exit


object StartJob extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Enter the valid pipeline parameter<Kafka or PubSub>")
      exit(1)

    }

  args(0) match {
      case "Kafka" =>
        val config = new Config

        val scalaProducer = new ScalaProducer()
        scalaProducer.send(args, config.getKafkaProperties.getString("TOPIC"))

      case "PubSub" =>
        val pubsubConfig = new Config
        val streamingContext = new SparkSingleton
        val ssc = streamingContext.createSparkStreamingContext
        val pubsub = new PubSub()
        logger.warn("Creating the Dstream by calling consume method")
        val streamRecord = pubsub.consume(pubsubConfig.getPubSubProperties.getString("PROJECT_ID"),
          pubsubConfig.getPubSubProperties.getString("SUBSCRIPTION"), ssc,
          Some(pubsubConfig.getPubSubProperties.getString("TOPIC")))
        logger.warn("Creating the spark dataset from streaming source")
        pubsub.createDataSetFromStream(streamRecord)
        logger.warn("Starting spark streaming context...")
        ssc.start()
        logger.warn("Creating the checkpoint as location src/main/resources/checkpoint")
        ssc.checkpoint(pubsubConfig.getPubSubProperties.getString("CHECKPOINT"))
        ssc.awaitTermination()

      case _ => println("Enter the valid pipeline name <Kafka Or PubSub>")
    }
  }
}
