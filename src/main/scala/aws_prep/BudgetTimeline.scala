package aws_prep

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

object BudgetTimeline {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BudgetTimeline")
      .master("local[*]") // Adjust as needed
      .getOrCreate()

    // Define schema for budget_changes table
    val schema = new StructType()
      .add("advertiser_id", StringType)
      .add("change_date", DateType)
      .add("new_budget", IntegerType)

    // Assuming data is loaded from a file or database; for demo, create sample data
    val sampleData = Seq(
      ("adv1", java.sql.Date.valueOf("2026-03-15"), 1000),
      ("adv1", java.sql.Date.valueOf("2026-04-05"), 1200),
      ("adv1", java.sql.Date.valueOf("2026-04-15"), 800),
      ("adv2", java.sql.Date.valueOf("2026-04-01"), 500),
      ("adv2", java.sql.Date.valueOf("2026-04-20"), 700)
    )

    val budgetChanges = spark.createDataFrame(sampleData).toDF("advertiser_id", "change_date", "new_budget")

    // Generate dates for April 2026
    val dates = spark.range(0, 30)
      .select(date_add(lit("2026-04-01"), col("id").cast("int")).as("date"))

    // Get distinct advertisers
    val advertisers = budgetChanges.select("advertiser_id").distinct()

    // Cross join advertisers with dates
    val timeline = advertisers.crossJoin(dates)

    // Join with budget_changes to find the last change date <= current date
    val withLastChange = timeline
      .join(budgetChanges,
        timeline("advertiser_id") === budgetChanges("advertiser_id") &&
        timeline("date") >= budgetChanges("change_date"),
        "left")
      .groupBy("advertiser_id", "date")
      .agg(max("change_date").as("last_change_date"))

    // Join back to get the new_budget for the last change
    val result = withLastChange
      .join(budgetChanges,
        withLastChange("advertiser_id") === budgetChanges("advertiser_id") &&
        withLastChange("last_change_date") === budgetChanges("change_date"),
        "left")
      .select("advertiser_id", "date", coalesce(col("new_budget"), lit(0)).as("active_budget"))
      .orderBy("advertiser_id", "date")

    // Show the result
    result.show(150) // Show all rows for April

    spark.stop()
  }
}