import org.apache.spark.sql.functions._

object SingleRecordProcess extends SparkSessionManager {

  val singleRecordFile = "src/main/resources/single_record.csv"

  def main(args : Array[String]): Unit = {
    implicit val sparkSession = startSession("Single Record")
    import sparkSession.implicits._
    val rdd = sparkSession.read.text(singleRecordFile)
    val exploded = rdd.as[String].flatMap(line => {
      val tokens = line.split("\\|")
      tokens.grouped(2).collect{
        case Array(name, age) => (name, age)
      }
    }).toDF("Name","Age")
    exploded.show(false)
  }
}
