import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.catalyst.expressions.DenseRank

object AdSpent {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("Ad Spent Analysis").getOrCreate()

        val ad_spend_events = spark.read.format("csv").option("header","true").load("src/main/resources/aws_prep/ad_spend_events.csv")

        val windowSpec = Window.partitionBy("advertiser_id").orderBy("timestamp")

        val resultData = ad_spend_events
        .withColumn("rolling_sum", sum("spend_amount").over(windowSpec))    
        .filter(col("rolling_sum") === 1000)
        .withColumn("Rank", dense_rank().over(windowSpec))
        .filter(col("Rank") === 1)
        .select("advertiser_id", "timestamp", "rolling_sum")            
    }
}