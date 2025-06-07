import FuturesImplementation.startSession
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions.DenseRank
import org.apache.spark.sql.expressions.Window

import scala.concurrent.Future

object Demo extends SparkSessionManager{

  val csvFilePath =  "src/main/resources/demo.csv"
  var txtFilePath = "src/main/resources/WordCountText.txt"

  def main(args: Array[String]): Unit = {
    implicit  val sparkSession = startSession("FuturesImplementation")

    val wordCount = wordCountFromTextFile(txtFilePath)
    println(s"WORD COUNT FROM THE FILE IS ${wordCount}")

    val sampleData = createDataFrameFromRdd()
    println("DATA CONVERTED FROM RDD TO DATA FRAME")
    sampleData.show(truncate = false)
    val csvData = readCsv()

    val highestSalaryInEachDepartment = findHighestSalaryFromEachDepartmentUsingGroupBy(csvData)
    println("HIGHEST SALARY OF EACH DEPARTMENT")
    highestSalaryInEachDepartment.show(truncate = false)

    val highestSalary = findHighestSalaryInEachDepartmentUsingWinFunc(csvData)
    println("HIGHEST SALARY USING WINDOW FUCNCTION")
    highestSalary.show(truncate = false)

    val highestSalaryOnly = findHighestSalaryOnly(csvData)
    println("HIGHEST SALARY USING TAKE")
    highestSalaryOnly.show(truncate = false)
    spark.close()
  }

  /**
   * Create sample dataframe from an rdd.
   * @param spark
   * @return
   */
  def createDataFrameFromRdd() : DataFrame = {
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
  def readCsv() : DataFrame ={
    val df = spark.read.format("csv").option("header",true).option("delimiter",",").load(csvFilePath)
    df
  }

  /**
   * This function finds the highest salary from employees csv data.
   * @param csvData
   * @param spark
   * @return
   */
  def findHighestSalaryInEachDepartmentUsingWinFunc(csvData : DataFrame) : DataFrame = {
    val data = csvData.withColumn("Salary",col("Salary").cast(IntegerType))
    val winSpec = Window.partitionBy("Department").orderBy(desc("Salary"))
    val dataWithRank = data.withColumn("Rank", rank().over(winSpec))
    val result = dataWithRank.filter(col("Rank") === lit(1))
    result.drop("Rank")
  }

  /**
   * Find highest salary using traditional groupby approach
   * @param csvData
   * @param spark
   * @return
   */
  def findHighestSalaryFromEachDepartmentUsingGroupBy(csvData: DataFrame) : DataFrame = {
    val data = csvData.withColumn("Salary",col("Salary").cast(IntegerType))
    val maxSalData = data.withColumnRenamed("Department","MaxSalDeparment")
      .groupBy("MaxSalDeparment")
      .agg(max("Salary").alias("MaxSalary"))
    val result = data.join(maxSalData,data("Salary") === maxSalData("MaxSalary")).select("EmpId","Name","Department","Salary")
    result
  }

  /**
   * Find employee having highest salary from complete employee data
   * @param csvData
   * @param spark
   * @return
   */
  def findHighestSalaryOnly(csvData : DataFrame) : DataFrame = {
    val data = csvData.withColumn("Salary",col("Salary").cast(IntegerType))
    val maxVal = data.agg(max("Salary")).take(1)(0)(0)
    val result = data.filter(col("Salary") === lit(maxVal))
    result
  }

  /**
   * Function to do word count from a csv file
   * @param spark
   */
  def wordCountFromTextFile(txtFile : String = txtFilePath): Long = {
    val fileContent = spark.sparkContext.textFile(txtFilePath)
    val result = fileContent.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _)
    result.collect().foreach(x => println(x._1 + " -> " + x._2))
    result.count()
  }

  /**
   * This function is used to read multi delimited file
   */
  def readMultiDelimitedFile(): Unit = {

  }

}