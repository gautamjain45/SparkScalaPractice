import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DemoTest extends FunSuite {

  implicit val spark = SparkSession.builder().master("local").appName("DemoTest").getOrCreate()
  val csvFilePath = "src/test/resources/demo.csv"
  val txtFilePath = "src/test/resources/WordCountText.txt"

  test("Demo.readCsv"){
    assert(Demo.readCsv().count() === 3)
  }

  test("Demo.findHighestSalaryFromEachDepartment"){
    val csvData = spark.read.format("csv").option("header",true).option("delimiter",",").load(csvFilePath)
    assert(Demo.findHighestSalaryFromEachDepartment(csvData).count() === 3)
  }

  test("Demo.findHighestSalary"){
    val csvData = spark.read.format("csv").option("header",true).option("delimiter",",").load(csvFilePath)
    assert(Demo.findHighestSalary(csvData).count() === 1)
  }

  test("Demo.wordCountFromTextFile"){
    assert(Demo.wordCountFromTextFile(txtFilePath) >= 0)
  }
}
