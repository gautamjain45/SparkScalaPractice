import org.apache.spark.sql.SparkSession

trait SparkSessionManager {
  var spark : SparkSession = _

  /**
   * Create spark session
   * @param appName
   * @return
   */
  def startSession(appName : String) : SparkSession = {
    spark = SparkSession.builder().master("local[*]").appName(appName).getOrCreate()
    println("SPARK SESSION CREATED!!")
    spark
  }

  def stopSession(): Unit = {
    if(spark != null) spark.close()
    println("SPARK SESSION STOPED!!")
  }
}
