import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions.DenseRank
import org.apache.spark.sql.expressions.Window

object Demo{

  implicit val spark : SparkSession = SparkSession.builder().master("local").appName("Demo").getOrCreate()

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sampleData = createDataFrameFromRdd()
    val csvData = readCsv()
    val highestSalaryInEachDepartment = findHighestSalaryFromEachDepartment(csvData)
    val highestSalary = findHighestSalary(csvData)
    highestSalary.show()
    spark.close()
  }

  /**
   * Create sample dataframe from an rdd.
   * @param spark
   * @return
   */
  def createDataFrameFromRdd()(implicit spark : SparkSession) : DataFrame = {
    val rdd = spark.sparkContext.parallelize(Seq(Row(1),Row(2),Row(3),Row(4)))
    val rdd1 = rdd.coalesce(2)
    val schema = new StructType().add(StructField("Numbers", IntegerType, true))
    val df = spark.createDataFrame(rdd1,schema)
    df
  }

  /**
   * Funtion to read csv data
   * @param spark
   * @return
   */
  def readCsv()(implicit spark : SparkSession) : DataFrame ={
    val df = spark.read.format("csv").option("header",true).option("delimiter",",").load("src/main/resources/demo.csv")
    df
  }

  /**
   * This function finds the highest salary from employees csv data.
   * @param csvData
   * @param spark
   * @return
   */
  def findHighestSalaryFromEachDepartment(csvData : DataFrame)(implicit spark : SparkSession) : DataFrame = {
    val data = csvData.withColumn("Salary",col("Salary").cast(IntegerType))
    val winSpec = Window.partitionBy("Department").orderBy(desc("Salary"))
    val dataWithRank = data.withColumn("Rank", rank().over(winSpec))
    val result = dataWithRank.filter(col("Rank") === lit(1))
    result
  }

  /**
   * Find employee having highest salary from complete employee data
   * @param csvData
   * @param spark
   * @return
   */
  def findHighestSalary(csvData : DataFrame)(implicit spark : SparkSession) : DataFrame = {
    val data = csvData.withColumn("Salary",col("Salary").cast(IntegerType))
    val maxVal = data.agg(max("Salary")).take(1)(0)(0)
    val result = data.filter(col("Salary") === lit(maxVal))
    result
  }

  /**
   * This function is used to read multi delimited file
   */
  def readMultiDelimitedFile(): Unit = {

  }

}