//////////////////////////////////////////////////////////////////////////////////
//
// File:        AllSchema.scala
// Description: Singletone class that contains all schemas that will be used
// Author:      Mostafa Mamdouh, Radwa Maher
// Created:     Wed Apr 07 13:33:12 PDT 2021
//
//////////////////////////////////////////////////////////////////////////////////


package dirlistener

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object AllSchema {

  // Define Schema for Segment file
  val segmentSchema = new StructType(Array(
    StructField("dial", StringType)
  ))

  // Define Schema for Rule file
  val ruleSchema = new StructType(Array(
    StructField("serviceId2", IntegerType),
    StructField("serviceName", StringType),
    StructField("startTime", IntegerType),
    StructField("endtTime", IntegerType),
    StructField("trafficLimit", IntegerType)
  ))

  // Define Schema for Data file
  val dataSchema = new StructType(Array(
    StructField("dial", StringType),
    StructField("serviceId", IntegerType),
    StructField("trafficVol", IntegerType),
    StructField("time", StringType)
  ))

}
