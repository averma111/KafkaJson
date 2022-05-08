package org.ashish.kafka.pubsub

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials, SparkPubsubMessage}
import org.ashish.kafka.dto.PubSubSchema
import org.ashish.kafka.pubsub.Pubsub.sparkConf
import org.slf4j.{Logger, LoggerFactory}
import sun.nio.cs.StandardCharsets

object Pubsub extends  App{

    val pubsub = new Pubsub()
    val sparkConf: SparkConf = new SparkConf().setAppName("Kafka-Pubsub").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Milliseconds(10000))
    val projectId ="kafka-pubsub-usecase"
    val subscription ="demo-topic-pubusb-sub"
    val topic="demo-topic-pubusb"

  val streamRecord= pubsub.consume(projectId,subscription,ssc,Option(topic))
  pubsub.bqWriter(streamRecord)

    ssc.start()
    ssc.checkpoint("src/main/resources/checkpoint")
    ssc.awaitTermination()
}
class Pubsub {
  implicit val topLevelObjectEncoder: Encoder[PubSubSchema] = Encoders.kryo[PubSubSchema]
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val credentialFilePath = "src/main/resources/credentials/keys.json"

  def consume(projectId: String, subscription: String, ssc: StreamingContext, topic: Option[String]): DStream[PubSubSchema] = {

      val pubsubStream = PubsubUtils.createStream(ssc, projectId, topic, subscription,
        SparkGCPCredentials.builder.jsonServiceAccount(credentialFilePath).build(),
        StorageLevel.MEMORY_AND_DISK_SER
      ).map(message => new PubSubSchema(new String(message.getData()), message.getMessageId()))
      pubsubStream

  }

  def bqWriter(message:DStream[PubSubSchema]):Unit = {
    message.foreachRDD{
      rdd => {
        val sparkSession = getInsance(rdd.sparkContext.getConf)
        val pubSubDataSet:Dataset[PubSubSchema] = sparkSession.createDataset(rdd).as[PubSubSchema]
        val stringPubSubDF =pubSubDataSet.selectExpr("CAST(value as STRING)").as(Encoders.BINARY)
        stringPubSubDF.select(col("value")).show(2,truncate = false)
      }
    }
  }

  def getInsance(sparkConf:SparkConf):SparkSession = {
    val sparkInstance = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkInstance
  }
}