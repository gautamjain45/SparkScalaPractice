object SeriesOfOperations extends SparkSessionManager {

  def main(args : Array[String]) : Unit = {
    /*val a = 10
    val b = 5
    val c = 0*/
    //val operations = "*/--+**"

    /*val result = applyOperations(a,b,operations)
    val singleDigitResult = reduceToSingleDigit(result)
    println(singleDigitResult)*/

    val d = 5
    val e = 3
    val f = 5
    val ops = "*/*++*-*"

    // Compute result with operations
    val intermediateResult = applyOperations(d, e, ops, f)
    println(intermediateResult)
  }

  def applyOperations(input1 : Int, input2 : Int, operation : String, count : Int = 0) : Int = {
    var result : Int = input1
    var incr = 0
    for(op <- operation) {
      incr = incr + 1
      result = op match {
        case '*' => result * input2
        case '/' => result / input2
        case '+' => result + input2
        case '-' => result - input2
        case _ => result
      }
      if (incr >= count && result > 9) result = reduceToSingleDigit(result)
      println(result)
    }
    result
  }

  def reduceToSingleDigit(n : Int) : Int = {
    var sum = n
    while(sum > 9){
      sum = sum.toString.map(_.asDigit).sum
    }
    sum
  }

}
