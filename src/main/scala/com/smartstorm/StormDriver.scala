package com.smartstorm


import com.datastax.driver.core.Cluster
import com.smartstorm.Message
import com.smartstorm.utils.CassandraUtils
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse
import com.smartstorm.utils.CassandraUtils
import com.smartstorm.utils.CassandraUtils.cluster



object StormDriver {

  private implicit val formats = DefaultFormats
  private val logger = Logger.getLogger(getClass)


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("KafkaSparkStreamingJob").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array("smart_msq")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(_.value)

    lines.foreachRDD(rdd => {
      logger.info("Partition size" + rdd.partitions.size)
      logger.info("Partition count" + rdd.count)
      logger.info("record => " + rdd.toString())

      rdd.foreach(
        record => {
          logger.info(record)
          val msg = parse(record.toString).extract[Message]
          logger.info("opening cassandra session")
          val session = cluster.connect("smartstorm")
          CassandraUtils.insertSensorData(msg,"sensors",session)
          logger.info("record inserted")

          session.close()
          logger.info("cassandra session closed")
        }
      )
    })

    lines.print()

    //val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)

    ssc.start()
    ssc.awaitTermination()
  }

}