package org.ashish.pubsub.consumer

import com.google.cloud.spark.bigquery.repackaged.com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.{BigQuery, BigQueryOptions, Job, JobInfo, QueryJobConfiguration}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.ashish.common.config.Getconfig
import org.ashish.common.spark.SparkSingleton
import org.ashish.impl.Logging
import org.ashish.pubsub.dto.PubSubSchema

import java.io.FileInputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.sys.exit

class PubSub extends Logging {
   val pubsubConfig = new Getconfig
  implicit val topLevelObjectEncoder: Encoder[PubSubSchema] = Encoders.product[PubSubSchema]
  def consume(projectId: String, subscription: String, ssc: StreamingContext, topic: Option[String]): DStream[PubSubSchema] = {
    val pubsubStream = PubsubUtils.createStream(ssc, projectId, topic, subscription,
      SparkGCPCredentials.builder.jsonServiceAccount(pubsubConfig.getPubSubProperties.getString("CREDENTIAL_FILE_PATH")).build(),
      StorageLevel.MEMORY_AND_DISK_SER
    ).map(message =>
      PubSubSchema(message.getData(), message.getMessageId(), message.getPublishTime())
    )
    if(pubsubStream == null){
      logger.warn("Dstream object is not instantiated.Exiting the process")
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
    val sparkConf = new SparkSingleton
    message.foreachRDD {
      rdd => {
        val sparkSession = sparkConf.getInstance(rdd.sparkContext.getConf)
        val pubSubDataSet: Dataset[PubSubSchema] = sparkSession.createDataset(rdd).as[PubSubSchema]
        val stringPubSubDF = pubSubDataSet.selectExpr("CAST(recordData as STRING)")
        val valueDf = stringPubSubDF.select(from_json(functions.col("recordData"), messageSchema)
          .as("recordData"))
        val messageDf = valueDf.select(functions.col("recordData.name"),
          functions.col("recordData.eventid"),
          functions.col("recordData.ingestionTs"))
        logger.warn("Calling dataframe writer method")
        writeDataFrameToBQ(messageDf)
      }
    }
  }

  def writeDataFrameToBQ(recordsDf: DataFrame): Unit = {
    val bigquery: BigQuery = BigQueryOptions.newBuilder().setProjectId(pubsubConfig.getPubSubProperties.getString("PROJECT_ID"))
      .setCredentials(
        ServiceAccountCredentials.fromStream(new FileInputStream(pubsubConfig.getPubSubProperties.getString("CREDENTIAL_FILE_PATH")))
      ).build().getService;
    if(bigquery == null) {
      logger.warn("Failed to create the bigquery client.Exiting the process")
      exit(1)
    }
  val count:Long = recordsDf.count()
    logger.warn(s"The record count processed is ${count}")
    val name = recordsDf.select("name").collect.map(f => f.getString(0)).toList
    val eventid = recordsDf.select("eventid").collect.map(f => f.getString(0)).toList
    val ingestionTs = recordsDf.select("ingestionTs").collect.map(f => f.getString(0)).toList
    val loadTs = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.ms").format(LocalDateTime.now())

    for (recordName <- name; recordEvent <- eventid;recordTs<-ingestionTs) {
      val query: String = s"INSERT INTO `${pubsubConfig.getPubSubProperties.getString("PROJECT_ID")}" +
        s".${pubsubConfig.getPubSubProperties.getString("DATASET_ID")}" +
        s".${pubsubConfig.getPubSubProperties.getString("TABLE_NAME")}` " +
        s"VALUES('${recordName}','${recordEvent}','${recordTs}','${loadTs}');"
      val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(query).build
      if(queryConfig == null) {
        logger.warn("Failed to create the queryConfig client.Exiting the process")
        exit(1)
      }
      val queryJob: Job = bigquery.create(JobInfo.newBuilder(queryConfig).build()).waitFor()
      if(queryJob == null) {
        logger.warn("Failed to create the queryJob client.Exiting the process")
        exit(1)
      }
      if (queryJob.getStatus.getError != null) throw new Exception(queryJob.getStatus.getError.toString)
      println("Rows inserted")
    }
  }


}
