package org.ashish.kafka.pubsub

import com.google.cloud.spark.bigquery.repackaged.com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.{BigQuery, BigQueryOptions,Job, JobInfo, QueryJobConfiguration}
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
import scala.sys.exit

object PubSub extends App {

  val projectId = "kafka-pubsub-usecase"
  val subscription = "demo-topic-pubusb-sub"
  val topic = "demo-topic-pubusb"

  lazy val LOGGER: Logger = LoggerFactory.getLogger(PubSub.getClass)
  val pubsub = new PubSub()
  val sparkConf: SparkConf = new SparkConf().setAppName("Kafka-Pubsub").setMaster("local[*]")
  if(sparkConf == null) {
    LOGGER.warn("Failed to create the spark conf.Exiting the process")
    exit(1)
  }
  val ssc = new StreamingContext(sparkConf, Milliseconds(10000))
  if(ssc == null) {
    LOGGER.warn("Failed to create the streaming context.Exiting the process")
    exit(1)
  }
  LOGGER.warn("Creating the dstream by calling consume method")
  val streamRecord = pubsub.consume(projectId, subscription, ssc, Option(topic))
  LOGGER.warn("Creating the spark dataset from streaming source")
  pubsub.createDataSetFromStream(streamRecord)
  ssc.start()
  LOGGER.warn("Creating the checkpoint as location src/main/resources/checkpoint")
  ssc.checkpoint("src/main/resources/checkpoint")
  ssc.awaitTermination()
}

class PubSub {
  lazy val LOGGER: Logger = LoggerFactory.getLogger(PubSub.getClass)

  private val credentialFilePath = "src/main/resources/credentials/keys.json"
  implicit val topLevelObjectEncoder: Encoder[PubSubSchema] = Encoders.product[PubSubSchema]
  def consume(projectId: String, subscription: String, ssc: StreamingContext, topic: Option[String]): DStream[PubSubSchema] = {
    val pubsubStream = PubsubUtils.createStream(ssc, projectId, topic, subscription,
      SparkGCPCredentials.builder.jsonServiceAccount(credentialFilePath).build(),
      StorageLevel.MEMORY_AND_DISK_SER
    ).map(message =>
      new PubSubSchema(message.getData(), message.getMessageId(), message.getPublishTime())
    )
    if(pubsubStream == null){
      LOGGER.warn("Dstream object is not instantiated.Exiting the process")
      exit(1)
    }
    pubsubStream

  }
  def createDataSetFromStream(message: DStream[PubSubSchema]): Unit = {
    val messageSchema = StructType(Array(
      StructField("name", StringType, nullable = true),
      StructField("eventId", StringType, nullable = true),
      StructField("ingestionTs",StringType,nullable = true)
    ))
    message.foreachRDD {
      rdd => {
        val sparkSession = getInstance(rdd.sparkContext.getConf)
        val pubSubDataSet: Dataset[PubSubSchema] = sparkSession.createDataset(rdd).as[PubSubSchema]
        val stringPubSubDF = pubSubDataSet.selectExpr("CAST(recordData as STRING)")
        val valueDf = stringPubSubDF.select(from_json(functions.col("recordData"), messageSchema)
          .as("recordData"))
        val messageDf = valueDf.select(functions.col("recordData.name"),
          functions.col("recordData.eventid"),
          functions.col("recordData.ingestionTs"))
        // messageDf.show(2, truncate = false)
        LOGGER.warn("Calling dataframe writer method")
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
    if(bigquery == null) {
      LOGGER.warn("Failed to create the bigquery client.Exiting the process")
      exit(1)
    }
  //  recordsDf.show(2,truncate= false)
  val count:Long = recordsDf.count()
    LOGGER.warn(s"The record count processed is ${count}")
    val name = recordsDf.select("name").collect.map(f => f.getString(0)).toList
    val eventid = recordsDf.select("eventid").collect.map(f => f.getString(0)).toList
    val ingestionTs = recordsDf.select("ingestionTs").collect.map(f => f.getString(0)).toList
    val loadTs = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.ms").format(LocalDateTime.now())

    for (recordName <- name; recordEvent <- eventid;recordTs<-ingestionTs) {
      val query: String = s"INSERT INTO `${projectId}.${dataSetId}.${table}` " +
        s"VALUES('${recordName}','${recordEvent}','${recordTs}','${loadTs}');"
      println(recordName,recordEvent,recordTs,loadTs)
      val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(query).build
      if(queryConfig == null) {
        LOGGER.warn("Failed to create the queryConfig client.Exiting the process")
        exit(1)
      }
      val queryJob: Job = bigquery.create(JobInfo.newBuilder(queryConfig).build()).waitFor()
      if(queryJob == null) {
        LOGGER.warn("Failed to create the queryJob client.Exiting the process")
        exit(1)
      }
     // if (queryJob == null) throw new Exception("job no longer exists")
      if (queryJob.getStatus.getError != null) throw new Exception(queryJob.getStatus.getError.toString)
      //val rowsInserted = queryJob.getStatistics
      println("Rows inserted")
    }
  }

  def getInstance(sparkConf: SparkConf): SparkSession = {
    val sparkInstance = SparkSession.builder().config(sparkConf).getOrCreate()
    if (sparkInstance == null) {
      LOGGER.warn("Failed to create the sparkInstance.Exiting the process")
      exit(1)
    }
    sparkInstance
  }
}
