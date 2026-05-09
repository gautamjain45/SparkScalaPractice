package aws_prep

/*The Challenge: "The Double Click"
Scenario:
Due to network lag or aggressive user behavior, the same "Ad Click" event is often sent to our Kinesis stream multiple times. If we count every event, we overbill the advertiser.

Task:
You are receiving a stream of ad clicks. You need to write a Spark Structured Streaming job in Scala that:

Deduplicates clicks based on click_id.

Handles "Late Arriving Data" using a 20-minute watermark.

Calculates the total Spend per Advertiser in 1-hour tumbling windows.*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object DeduplicatesClicks {
  
  val spark = SparkSession.builder()
    .appName("Deduplicates Clicks")
    .getOrCreate()

    def main(args: Array[String]): Unit = {

        import spark.implicits._

        val schema = new StructType()
        .add("click_id", StringType)
        .add("advertiser_id", IntegerType)
        .add("timestamp", TimestampType)
        .add("bid_amount", DoubleType)

        val dataEvents = spark.readStream
        .format("kinesis")
        .option("StreamName", "ad-clicks-stream")
        .option("endpointUrl", "https://kinesis.us-east-1.amazonaws.com")
        .load()

        val clicksDF = dataEvents
        .selectExpr("CAST(data AS STRING) as json")
        .select(from_json(col("json"), schema).as("data"))
        .select("data.*")   

        val processedDF = clicksDF.withWatermark("timestamp", "20 minutes")
        .dropDuplicates("click_id", "timestamp")
        .groupBy(window(col("timestamp"), "1 hour"), col("advertiser_id"))
        .agg(sum("bid_amount").alias("total_hourly_spend"))

        val query = processedDF.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .start()     

        query.awaitTermination()   
    }

}
