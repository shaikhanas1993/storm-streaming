package com.smartstorm

import org.apache.spark.sql.SparkSession

/**
  * Created by jwszol on 10/11/2017.
  */
class SparkRunner {

  def run(): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()


  }

}
