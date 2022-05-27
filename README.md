# KafkaJsonPubSubBQSpark
UseCase=> To push json data to a Kafka topic. Connect Kafka to Pubsub sink connector using Kafkaconnect. Read the data from pubsub and write the data into Google BQ

How to create the Jar
1.mvn clean package

How to run the Jar

1.Kafka => java -cp KafkaJson-1.0-SNAPSHOT-jar-with-dependencies.jar org.ashish.driver.StartJob Kafka

2.PubSub => java -cp KafkaJson-1.0-SNAPSHOT-jar-with-dependencies.jar org.ashish.driver.StartJob PubSub

