import FuturesImplementation.startSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.With
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}
object FutureAndPromiseImplementation extends SparkSessionManager {
  implicit var sparkSession : SparkSession = _
  var txtFilePath = "src/main/resources/WordCountText.txt"
  val csvFilePath = "src/main/resources/demo.csv"
  def main(args: Array[String]): Unit = {
    sparkSession = startSession("FuturesAndPromiseImplementation")

    //Calling future function with promise
    val futureWordCount = futureWordCountAsync(txtFilePath)
    futureWordCount.onComplete({
      case Success(count) => println(s"WORD COUNT IS ${count}")
      case Failure(exception) => println(s"ERROR OCCURRED : ${exception.getMessage}")
    })

    //Calling normal funciton to see which output is received first
    val df = createDataframeFromRdd()
    df.show(truncate = false)

    stopSession()
  }

  def futureWordCountAsync(textFilePath : String) : Future[Int] = {
    val promise = Promise[Int]()
    Future {
      if(textFilePath.isEmpty)promise.failure(new RuntimeException(s"CAN NOT READ FILE FROM EMPTY PATH"))
      val rdd = sparkSession.sparkContext.textFile(textFilePath)
      val wordCount = rdd.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _)
      val wordCountFormatted = wordCount.collect().map(x=> x._1 + "->" + x._2 + "\n").mkString("")
      println(s"FORMATTED WORD COUNT IS ${wordCountFormatted}")
      promise.success(wordCount.count().toInt)
    }
    promise.future
  }

  def createDataframeFromRdd()(implicit spark: SparkSession): DataFrame = {
    val rdd = spark.sparkContext.parallelize(Seq(Row(1, "Gautam"), Row(2, "Rahul"), Row(3, "Sachin"), Row(4, "Pawan"))).coalesce(1)
    val schema = new StructType().add(StructField("Id", IntegerType, true)).add(StructField("Name", StringType, true))
    val dataFrame = spark.createDataFrame(rdd, schema)
    dataFrame
  }

}
