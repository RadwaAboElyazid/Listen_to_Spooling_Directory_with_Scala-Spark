package spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DirListener {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Start Session
    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")
      .master("local[*]")
      .getOrCreate()

    // Define Schema for Segment
    val segmentSchema = new StructType(Array(
      StructField("dial", StringType)
    ))

    // Read Segment file
    val segment = spark.read
      .format("csv")
      .option("header", "false")
      .schema(segmentSchema)
      .load("data/SEGMENT.csv")

    // Define Schema for Rule file
    val ruleSchema = new StructType(Array(
      StructField("serviceId2", IntegerType),
      StructField("serviceName", StringType),
      StructField("startTime", IntegerType),
      StructField("endtTime", IntegerType),
      StructField("trafficLimit", IntegerType)
    ))

    // Read Rule file
    val rule = spark.read
      .format("csv")
      .option("header", "false")
      .option("sep", ",")
      .schema(ruleSchema)
      .load("data/RULES.csv")

    //---------------------------------------------------------------------------------------------//

    // Define Schema for Data
    val dataSchema = new StructType(Array(
      StructField("dial", StringType),
      StructField("serviceId", IntegerType),
      StructField("trafficVol", IntegerType),
      StructField("time", StringType)
    ))

    // Listen to the directory
    val initData = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("sep", ",")
      .schema(dataSchema)
      .load("data/logs")

    // From String to TimeStamp & extract hour and date
    val data = initData.withColumn("timestamp",
      to_timestamp(col("time"),"yyyyMMddHHmmss"))
      .withColumn("hour", hour(col("timestamp")))
      .withColumn("date", to_date(col("timestamp")))
      .withColumn("hour_minute", substring(col("time"), 9, 4))

    // Join data & segment
    val dataSegment = data.join(segment, "dial")

    // Join dataSegment & rules
    val dataSegmentRule = dataSegment.join(rule, dataSegment("serviceId") === rule("serviceId2")
      && dataSegment("trafficVol") > rule("trafficLimit")
      && dataSegment("hour") >= rule("startTime")
      && dataSegment("hour") <= rule("endtTime"), "inner")

    // Select desired cols
    val prepareResult = dataSegmentRule.select("dial","serviceId","trafficVol", "time", "timestamp", "date", "hour_minute", "serviceName")

    // Create empty appended dataframe with the same schema
    var appendedDateFrame = spark.createDataFrame(spark.sparkContext
      .emptyRDD[Row], prepareResult.schema)

    // For each batch stream
    val updateQuery = prepareResult
      .writeStream
      .outputMode("append")
      .format("console")
      .foreachBatch((batch: DataFrame, batchId: Long) => {
        // Define logic
        val logic = batch.join(appendedDateFrame,batch("serviceId") ===  appendedDateFrame("serviceId")
          && batch("dial") ===  appendedDateFrame("dial")
          && batch("date") === appendedDateFrame("date")
          ,"leftanti")

        // Appended Dataframe
        appendedDateFrame = appendedDateFrame.union(logic)
        appendedDateFrame.show()

        // To prevent OOM error, free the data frame at the end of everyday
        if(appendedDateFrame.filter(col("hour_minute") === "2359").count() > 0) {
          appendedDateFrame = spark.createDataFrame(spark.sparkContext
            .emptyRDD[Row], prepareResult.schema)
        }

        // Select only the desired cols
        val finalResult = logic.select("dial","serviceId","trafficVol", "time", "serviceName")

        // Write stream to files
        finalResult.coalesce(1)
          .write
          .partitionBy("serviceName")
          .format("csv")
          .option("checkpointLocation", "data/checkpoint/")
          .mode("append")
          .save("data/output/")
      })
      .option("checkpointLocation", "data/checkpoint").start()

    // Wait until we terminate the scripts
    updateQuery.awaitTermination()

    // Stop the session
    spark.stop()
  }

}
