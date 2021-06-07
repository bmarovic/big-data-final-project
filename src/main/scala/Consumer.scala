import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.apache.spark.sql.SQLContext.getOrCreate
import org.elasticsearch.spark.sql.sparkDatasetFunctions

object Consumer {
  def main(args: Array[String]):Unit= {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .config(ConfigurationOptions.ES_NODES, "127.0.0.1")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .master("local[2]")
      .appName("KafkaConsumer")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

    sc.setLogLevel("Error")

    val topicSet = Set("bolt")
    val kafkaParm = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "group",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val events = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParm))

    val data = sc.textFile("/Users/Bruno/IdeaProjects/BigDataFinalProject/project.txt").filter(!_.startsWith(",")).map(l=>l.split(","))
    val intData=data.map(l=>l.map(elm=>elm.toInt))
    val parsedData=intData.map(l=>{
      val features=Vectors.dense(l(3),l(4),l(5),l(6),l(7),l(8))
      LabeledPoint(l(10),features)

    }).cache()

    val dtmodel=DecisionTree.trainClassifier(parsedData,2,Map[Int,Int](),"gini",5,32)
    val samples = events.map(_.value()).map(l=>{
      val sample = Vectors.dense(l.split(",").map(_.toDouble))
      val prediction = dtmodel.predict(sample)

      (sample(0).toInt, sample(1).toInt, sample(2).toInt,
        sample(3).toInt, sample(4).toInt, sample(5).toInt, prediction.toInt)
    })

    samples.foreachRDD( rdd =>
      { rdd.saveAsTextFile("src/main/logs_cons")
        val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
        val rdd2df = sqlContext.createDataFrame(rdd)
          .toDF("StatusCount", "FollowerCount", "FriendCount", "FavouritesCount", "Protected", "Verified", "BoltPrediction")
        println(rdd2df.head())
        rdd2df.saveToEs("final/test")
      }
    )
    samples.print()
    ssc.start()
    ssc.awaitTermination()

  }}
