package com.smartstorm.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.datastax.driver.core.{Cluster, ResultSet, Session}
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder => QB}
import com.smartstorm.Message
import org.joda.time.{DateTime, DateTimeZone}

case class Sensor(id: String,
                  created: String,
                  duration: Int
                 )


/**
  * Created by jwszol on 16/11/2017.
  */
object CassandraUtils {

  val cluster = {
    Cluster.builder()
      .addContactPoint("localhost")
      .build()
  }


  def insertSensorData(message: Message, table: String, session: Session): Unit = {
    val format = new SimpleDateFormat("d-M-y hh:mm:ss")
    val timestamp = format.format(Calendar.getInstance().getTime())

    val insertQuery = QB.insertInto(table)
      .value("sensorid", message.sensor_id)
      .value("userid",message.user_id)
      .value("sensordesc", message.desc)
      .value("value",message.value)
      .value("created", timestamp)
      .using(QB.ttl(100))

    session.execute(insertQuery)
  }


  def getSensorData(id: String, session : Session) : ResultSet = {

    val slectQuery = QB.select("id", "created","duration")
      .from("test")
      .where(QB.eq("id",id))

    session.execute(slectQuery)
  }


}
