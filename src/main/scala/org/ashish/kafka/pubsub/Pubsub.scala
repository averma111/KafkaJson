package org.ashish.kafka.pubsub

import com.google.cloud.spark.bigquery.repackaged.com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.{BigQuery, BigQueryError, BigQueryOptions, BigtableColumn, InsertAllRequest, InsertAllResponse, Job, JobInfo, QueryJobConfiguration, TableId}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.ashish.kafka.dto.PubSubSchema
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileInputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.math



object Pubsub extends App {

  val pubsub = new Pubsub()
  val sparkConf: SparkConf = new SparkConf().setAppName("Kafka-Pubsub").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Milliseconds(10000))
  val projectId = "kafka-pubsub-usecase"
  val subscription = "demo-topic-pubusb-sub"
  val topic = "demo-topic-pubusb"

  val streamRecord = pubsub.consume(projectId, subscription, ssc, Option(topic))
  pubsub.createDataSetFromStream(streamRecord)

  ssc.start()
  ssc.checkpoint("src/main/resources/checkpoint")
  ssc.awaitTermination()
}

class Pubsub {
  implicit val topLevelObjectEncoder: Encoder[PubSubSchema] = Encoders.product[PubSubSchema]
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val credentialFilePath = "src/main/resources/credentials/keys.json"

  def consume(projectId: String, subscription: String, ssc: StreamingContext, topic: Option[String]): DStream[PubSubSchema] = {

    val pubsubStream = PubsubUtils.createStream(ssc, projectId, topic, subscription,
      SparkGCPCredentials.builder.jsonServiceAccount(credentialFilePath).build(),
      StorageLevel.MEMORY_AND_DISK_SER
    ).map(message =>
      new PubSubSchema(message.getData(), message.getMessageId(), message.getPublishTime())
    )
    pubsubStream

  }

  def createDataSetFromStream(message: DStream[PubSubSchema]): Unit = {
    val messageSchema = StructType(Array(
      StructField("name", StringType, true),
      StructField("eventId", StringType, true),
      StructField("ingestionTs",StringType,true)
    ))
    message.foreachRDD {
      rdd => {
        val sparkSession = getInsance(rdd.sparkContext.getConf)
        val pubSubDataSet: Dataset[PubSubSchema] = sparkSession.createDataset(rdd).as[PubSubSchema]
        val stringPubSubDF = pubSubDataSet.selectExpr("CAST(recordData as STRING)")
        val valueDf = stringPubSubDF.select(from_json(functions.col("recordData"), messageSchema)
          .as("recordData"))
        val messageDf = valueDf.select(functions.col("recordData.name"),
          functions.col("recordData.eventid"),
          functions.col("recordData.ingestionTs"))
        // messageDf.show(2, truncate = false)
        writeDataFrameToBQ(messageDf)
      }
    }
  }

  def writeDataFrameToBQ(recordsDf: DataFrame): Unit = {
    // val bucketName = "pubsubtempbucket"
    val dataSetId = "fastmessage"
    val table = "streamingfastmessages"
    val projectId = "kafka-pubsub-usecase"

    val bigquery: BigQuery = BigQueryOptions.newBuilder().setProjectId(projectId)
      .setCredentials(
        ServiceAccountCredentials.fromStream(new FileInputStream(credentialFilePath))
      ).build().getService;
    recordsDf.show(2,truncate= false)
    val name = recordsDf.select("name").collect.map(f => f.getString(0)).toList
    val eventid = recordsDf.select("eventid").collect.map(f => f.getString(0)).toList
    val ingestionTs = recordsDf.select("ingestionTs").collect.map(f => f.getString(0)).toList.toString()
    val loadTs = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.ms").format(LocalDateTime.now())
    for (recordName <- name; recordEvent <- eventid;recordTs<-ingestionTs) {
      val query: String = s"INSERT INTO `${projectId}.${dataSetId}.${table}` " +
        s"VALUES('${recordName}','${recordEvent}','${recordTs}','${loadTs}');"
      print(recordName,recordEvent,recordTs,loadTs)
      val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(query).build
      val queryJob: Job = bigquery.create(JobInfo.newBuilder(queryConfig).build()).waitFor()
      if (queryJob == null) throw new Exception("job no longer exists")
      if (queryJob.getStatus.getError != null) throw new Exception(queryJob.getStatus.getError.toString)
      //val rowsInserted = queryJob.getStatistics
      println("Rows inserted")
    }
  }

  def getInsance(sparkConf: SparkConf): SparkSession = {
    val sparkInstance = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkInstance
  }

}