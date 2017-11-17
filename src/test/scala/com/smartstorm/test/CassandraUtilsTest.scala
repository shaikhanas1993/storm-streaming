package com.smartstorm.test

import com.datastax.driver.core.Row
import com.smartstorm.Message
import com.smartstorm.utils.CassandraUtils
import com.smartstorm.utils.CassandraUtils.cluster
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, GivenWhenThen}

import scala.collection.JavaConverters._

/**
  * Created by jwszol on 16/11/2017.
  */
class CassandraUtilsTest extends FunSpec with GivenWhenThen with MockFactory {

  describe("CassandraUtils - get the data from DB") {
    it("Should connect and get testing data") {

//      val session = cluster.connect("smartstorm")
//      val msg = new Message("user_id_123","sensor_id_123","testing_desc", "100")
//      CassandraUtils.insertSensorData(msg,"sensors",session)

//      val dataRS = CassandraUtils.getSensorData("sensor12", session)
//
//      val all = dataRS.all().asScala.toList
//
//      all.foreach(x =>
//        println(x.getString("id"))
//      )

//      session.close()
    }
  }
}
