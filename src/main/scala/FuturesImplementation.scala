import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object FuturesImplementation extends SparkSessionManager {

  val csvFilePath = "src/main/resources/demo.csv"
  var txtFilePath = "src/main/resources/WordCountText.txt"

  def main(args : Array[String]) : Unit = {
    //Calling future function
    implicit  val sparkSession = startSession("FuturesImplementation")
    val futureWordCount = WordCountFromTextFileAsync(txtFilePath)
    futureWordCount.onComplete{
      case Success(count) => println(s"WORD COUNT FROM THE FILE IS ${count}")
      case Failure(exception) => println(s"ERROR OCCURRED WHILE DOING WORD COUNT: ${exception.getMessage}")
    }

    //Calling normal funciton to see which output is received first
    val df = createDataframeFromRdd()
    df.show(truncate = false)

    Await.result(futureWordCount, 10.seconds)
    spark.close()
  }

  /**
   * Created word count function using futures
   * @param textFilePath
   * @param spark
   * @return
   */
  def WordCountFromTextFileAsync(textFilePath : String) : Future[Int] = {
    Future {
      //Adding some delay
      Thread.sleep(2000)
      if (textFilePath.isEmpty) throw new RuntimeException("Can not read file from empty file path.")
      val fileContent = spark.sparkContext.textFile(textFilePath)
      val wordsWithCount = fileContent.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
      val wordCountFormatted = wordsWithCount.collect().foldLeft("") { (acc, x) => acc + x._1 + " -> " + x._2 + "\n" }.mkString("")
      println(wordCountFormatted)
      wordsWithCount.count().toInt
    }
  }

  /**
   * Create dataframe out of rdd
   * @param spark
   * @return
   */
  def createDataframeFromRdd()(implicit spark: SparkSession) : DataFrame = {
    val rdd = spark.sparkContext.parallelize(Seq(Row(1, "Gautam"), Row(2,"Rahul"), Row(3, "Sachin"), Row(4, "Pawan"))).coalesce(1)
    val schema = new StructType().add(StructField("Id",IntegerType, true)).add(StructField("Name", StringType, true))
    val dataFrame = spark.createDataFrame(rdd,schema)
    dataFrame
  }

}
