package com.smartstorm.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.datastax.driver.core.{Cluster, ResultSet, Session}
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder => QB}
import com.smartstorm.Message
import com.smartstorm.Config
import org.joda.time.{DateTime, DateTimeZone}



/**
  * Created by jwszol on 16/11/2017.
  */
object CassandraUtils {

  val cluster = {
    Cluster.builder()
      .addContactPoint(Config.ContactPointAddress)
      .build()
  }


  def insertSensorData(message: Message, table: String, session: Session): Unit = {
    val format = new SimpleDateFormat("d-M-y hh:mm:ss")
    val timestamp = format.format(Calendar.getInstance().getTime())
    val timeEpoch = Calendar.getInstance().getTimeInMillis()/1000L

    val insertQuery = QB.insertInto(table)
      .value("sensorid", message.sensorId)
      .value("userid",message.userId)
      .value("sensordesc", "test desc")
      .value("value",message.value)
      .value("created", timestamp)
      .value("created_epoch",timeEpoch)
      .using(QB.ttl(86400))

    session.execute(insertQuery)
  }


  def getSensorData(id: String, session : Session) : ResultSet = {

    val slectQuery = QB.select("id", "created","duration")
      .from("test")
      .where(QB.eq("id",id))

    session.execute(slectQuery)
  }


}
