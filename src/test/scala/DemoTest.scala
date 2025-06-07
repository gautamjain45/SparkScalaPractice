import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DemoTest extends FunSuite with SparkSessionManager {

  implicit val sparkSession: SparkSession = startSession("Demo test")
  val csvFilePath = "src/test/resources/demo.csv"
  val txtFilePath = "src/test/resources/WordCountText.txt"

  test("Demo.readCsv"){
    assert(Demo.readCsv().count() === 3)
  }

  test("Demo.findHighestSalaryFromEachDepartment"){
    val csvData = sparkSession.read.format("csv").option("header",true).option("delimiter",",").load(csvFilePath)
    assert(Demo.findHighestSalaryFromEachDepartmentUsingGroupBy(csvData).count() === 3)
  }

  test("Demo.findHighestSalary"){
    val csvData = sparkSession.read.format("csv").option("header",true).option("delimiter",",").load(csvFilePath)
    assert(Demo.findHighestSalaryInEachDepartmentUsingWinFunc(csvData).count() === 1)
  }

  test("Demo.wordCountFromTextFile"){
    assert(Demo.wordCountFromTextFile(txtFilePath) >= 0)
  }
}
