
import com.datastax.driver.core.Cluster
import com.smartstorm.Message
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

object StormDriver {

  private implicit val formats = DefaultFormats

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("KafkaHBaseWordCount").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val cluster = {
      Cluster.builder()
        .addContactPoint("localhost")
        // .withCredentials("username", "password")
        .build()
    }

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array("dl_msg")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(_.value)

    lines.foreachRDD(rdd => {
      println("Partition size" + rdd.partitions.size)
      println("Partition count" + rdd.count)

      println("rekord ===== " + rdd.toString())

      rdd.foreach(
        record => {
          println("rekord " + record)

          val jsonObjs = parse(record.toString).extract[Message]
          println("wartosc = " + jsonObjs.dataCollectionId)

        }
      )

    }

    )


    lines.print()

    //val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)





    ssc.start()
    ssc.awaitTermination()
  }

}