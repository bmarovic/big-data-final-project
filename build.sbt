name := "stream"

version := "0.1"

scalaVersion := "2.12.12"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.4.4" ,
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.8.0",
  //"org.elasticsearch" % "elasticsearch-spark_2.12" % "7.13.0",
  //"org.elasticsearch" % "elasticsearch-hadoop" % "7.11.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.4"


)
// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.13.0"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.5"
