import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import java.sql.DriverManager
import java.sql.Connection
import java.util.Properties

object ExactlyOnceKinesisToRedshift {

  def main(args: Array[String]): Unit = {

    // Assume args: appName, streamName, endpointUrl, regionName, checkpointDir, redshiftUrl, redshiftUser, redshiftPassword, tableName

    val appName = args(0)
    val streamName = args(1)
    val endpointUrl = args(2)
    val regionName = args(3)
    val checkpointDir = args(4)
    val redshiftUrl = args(5)
    val redshiftUser = args(6)
    val redshiftPassword = args(7)
    val tableName = args(8)

    val spark = SparkSession.builder()
      .appName(appName)
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    // Enable checkpointing
    ssc.checkpoint(checkpointDir)

    // Create Kinesis DStream
    val kinesisStream = KinesisInputDStream.builder
      .streamingContext(ssc)
      .streamName(streamName)
      .endpointUrl(endpointUrl)
      .regionName(regionName)
      .initialPositionInStream(InitialPositionInStream.LATEST)
      .checkpointAppName(appName)
      .checkpointInterval(Seconds(30))
      .build()

    // Process the stream
    kinesisStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._

        // Assuming the data is JSON strings
        val df = spark.read.json(rdd.map(_.toString).toDS())

        // Add a unique key if not present, e.g., using timestamp or UUID
        // For demo, assume data has 'id' as unique key
        // If not, generate one: val dfWithId = df.withColumn("unique_id", monotonically_increasing_id())

        // Write to Redshift with exactly-once semantics using UPSERT
        writeToRedshiftWithUpsert(df, redshiftUrl, redshiftUser, redshiftPassword, tableName)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def writeToRedshiftWithUpsert(df: DataFrame, url: String, user: String, password: String, table: String): Unit = {
    // Create a temporary table name for staging
    val tempTable = s"${table}_temp_${System.currentTimeMillis()}"

    // JDBC properties
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.amazon.redshift.jdbc.Driver")

    // Write to a temporary table in Redshift
    df.write
      .mode(SaveMode.Overwrite)
      .jdbc(url, tempTable, props)

    // Use JDBC connection to perform UPSERT
    val connection: Connection = DriverManager.getConnection(url, user, password)
    try {
      val statement = connection.createStatement()

      // Assuming the table has a primary key 'id'
      // Perform MERGE or INSERT ... ON CONFLICT
      val upsertQuery = s"""
        BEGIN;
        DELETE FROM $table WHERE id IN (SELECT id FROM $tempTable);
        INSERT INTO $table SELECT * FROM $tempTable;
        DROP TABLE $tempTable;
        COMMIT;
      """

      statement.execute(upsertQuery)
    } finally {
      connection.close()
    }
  }
}