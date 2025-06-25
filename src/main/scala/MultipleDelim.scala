import Demo.startSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions._

object MultipleDelim extends SparkSessionManager{

  val csvFilePath =  "src/main/resources/multiple_delim.csv"

  def main (args : Array[String]) : Unit = {
    implicit  val sparkSession = startSession("Multiple Delimiter")
    val rdd = sparkSession.read.text(csvFilePath).filter(!col("value").startsWith("Name"))
    val df = rdd.withColumn("tokens", split(col("value"), "~\\|"))
      .select(col("tokens").getItem(0).as("Name"),
      col("tokens").getItem(1).as("Age"))
    df.show(truncate = false)
    stopSession()
  }

}
