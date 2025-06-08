import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataSkewnessResolvedWithSalting extends SparkSessionManager {

  implicit val sparkSession: SparkSession = startSession("Salting")
  def main(args : Array[String]) : Unit = {
    val salesData = createSalesData()
    val storeData = createStoreData()
    val result = joinAndApplySalting(salesData, storeData)
    stopSession()
  }

  /**
   * Create sales data
   * @return
   */
  def createSalesData() : DataFrame = {
    import sparkSession.implicits._
    val salesData : DataFrame = Seq(
      (1001, "ProductA", 500),
      (1001, "ProductB", 300),
      (1001, "ProductC", 200),
      (1002, "ProductD", 150),
      (1003, "ProductE", 100)
    ).toDF("store_id","product", "sales")
    salesData
  }

  def createStoreData() : DataFrame = {
    import sparkSession.implicits._
    val storeData : DataFrame = Seq(
      (1001, "Store A", "New York"),
      (1002, "Store B", "Los Angeles"),
      (1003, "Store C", "Chicago")
    ).toDF("store_id", "store_name", "location")
    storeData
  }

  /**
   * Join two datasets using salting
   * @param salesData
   * @param storeData
   * @return
   */
  def joinAndApplySalting(salesData : DataFrame, storeData : DataFrame): DataFrame = {
    val saltRange : Int = 10
    val saltedSalesData = salesData.withColumn("salted_store_id",concat(col("store_id"), lit("_"), floor(rand() * saltRange)))
    saltedSalesData.show(truncate = false)
    val saltedStoreData = storeData.withColumn("salted_store_id", explode(array((0 to saltRange).map( x => concat(col("store_id"), lit("_"), lit(x))): _*))).drop("store_id")
    saltedStoreData.show(numRows = saltedStoreData.count().toInt,truncate = false)
    val joinedData = saltedSalesData.join(saltedStoreData, "salted_store_id")
    joinedData.drop("salted_store_id").show(truncate = false)
    joinedData
  }
}
