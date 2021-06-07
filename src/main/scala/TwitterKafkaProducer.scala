import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext.getOrCreate
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql.sparkDatasetFunctions

import java.util.Properties

object TwitterKafkaProducer{
  def main(args:Array[String]): Unit={
    Logger.getLogger("org").setLevel(Level.OFF)

    if (args.length == 0) {
      System.out.println("Enter the name of the topic")
      return
    }
    val topicName = args(0)

    System.setProperty("twitter4j.oauth.consumerKey", "-------------")
    System.setProperty("twitter4j.oauth.consumerSecret", "-------------")
    System.setProperty("twitter4j.oauth.accessToken", "--------------")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "------------")

    val spark = SparkSession.builder()
      .config(ConfigurationOptions.ES_NODES, "127.0.0.1")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .master("local[2]")
      .appName("kafka-twitter-streamer")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    val twitterStream = TwitterUtils.createStream(ssc, None)
    val twitterStreamEN = twitterStream.filter(t => t.getLang == "en")
      .map(t => (t.getUser.getStatusesCount, t.getUser.getFollowersCount, t.getUser.getFriendsCount, t.getUser.getFavouritesCount,
      t.getUser.isProtected, t.getUser.isVerified))

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    twitterStreamEN.foreachRDD(rdd=> {
      rdd.foreachPartition(l =>{
        val producer = new KafkaProducer[String, String](props)
        l.foreach { tweet =>
          val statusCount = tweet._1
          val followCount = tweet._2
          val friendCount = tweet._3
          val favCount = tweet._4
          val isProtected = if (tweet._5) "1" else "0"
          val isVerified = if (tweet._6) "1" else "0"
          //println(s"$statusCount,$followCount,$friendCount,$favCount,$isProtected,$isVerified")
          val record = new ProducerRecord(topicName, "key",
            s"$statusCount,$followCount,$friendCount,$favCount,$isProtected,$isVerified")
          producer.send(record)
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
