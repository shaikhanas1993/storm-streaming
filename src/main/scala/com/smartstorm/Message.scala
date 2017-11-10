package com.smartstorm

/**
  * Created by jwszol on 10/11/2017.
  */
case class Message (
                     dataCollectionId: String,
                     tenantId: String,
                     `type`: String,
                     message: String,
                     eventType: String,
                     coreId: String,
                     firstRun: String
                   )
