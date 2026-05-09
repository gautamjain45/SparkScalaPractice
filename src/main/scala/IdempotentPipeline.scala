import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

object IdempotentPipeline {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: IdempotentPipeline <date>")
      System.exit(1)
    }

    val processDate = args(0) // e.g., "2026-05-04"

    val spark = SparkSession.builder()
      .appName("Idempotent Revenue Pipeline")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Source auditing: Check if this date's data has already been processed
    val metadataPath = "target/delta/metadata"
    val outputPath = "target/delta/revenue"

    // Create metadata table if not exists
    if (!DeltaTable.isDeltaTable(spark, metadataPath)) {
      spark.range(0).toDF("dummy").write.format("delta").save(metadataPath)
      // Actually, create with schema
      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], 
        org.apache.spark.sql.types.StructType(Seq(
          org.apache.spark.sql.types.StructField("processed_date", org.apache.spark.sql.types.StringType, true),
          org.apache.spark.sql.types.StructField("file_name", org.apache.spark.sql.types.StringType, true)
        )))
      emptyDF.write.format("delta").save(metadataPath)
    }

    val metadataTable = DeltaTable.forPath(spark, metadataPath)

    // Check if already processed
    val isProcessed = metadataTable.toDF.filter(col("processed_date") === processDate).count() > 0

    if (isProcessed) {
      println(s"Data for $processDate already processed. Skipping.")
      spark.stop()
      return
    }

    // Read source data
    val sourceFile = "src/main/resources/demo.csv"
    val df = spark.read.option("header", "true").csv(sourceFile)

    // Deterministic transformation: Sum salaries as revenue, add date
    val revenueDF = df.withColumn("date", lit(processDate))
      .groupBy("date", "Department")
      .agg(sum("Salary").as("total_revenue"))

    // Atomic write: Overwrite partition
    revenueDF.write
      .format("delta")
      .mode("overwrite")
      .partitionBy("date")
      .save(outputPath)

    // Mark as processed
    import spark.implicits._
    val newMetadata = Seq((processDate, sourceFile)).toDF("processed_date", "file_name")

    metadataTable.as("meta")
      .merge(newMetadata.as("new"), "meta.processed_date = new.processed_date")
      .whenNotMatched()
      .insertAll()
      .execute()

    println(s"Processed data for $processDate successfully.")

    spark.stop()
  }
}