import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DemoTest extends FunSuite {

  implicit val spark = SparkSession.builder().master("local").appName("DemoTest").getOrCreate()

  test("Demo.readCsv"){
    assert(Demo.readCsv().count() === 3)
  }

  test("Demo.findHighestSalaryFromEachDepartment"){
    val csvData = spark.read.format("csv").option("header",true).option("delimiter",",").load("src/test/resources/demo.csv")
    assert(Demo.findHighestSalaryFromEachDepartment(csvData).count() === 3)
  }

  test("Demo.findHighestSalary"){
    val csvData = spark.read.format("csv").option("header",true).option("delimiter",",").load("src/test/resources/demo.csv")
    assert(Demo.findHighestSalary(csvData).count() === 1)
  }
}
