
object EpamTest extends App {

  var arr = Array(-1, -3)

  var maxVal = arr.max
  println("MAX VALUE IS " + maxVal)

  var minVal = arr.min
  println("MIN VALUE IS " + minVal)

  if(maxVal < 0 || minVal < 0) println("YES MAX AND MIN BOTH VALUES ARE LESS THAN 0 " + 1)

  var arrRange = Range(minVal, maxVal).toArray

  try{
    var result = arrRange.diff(arr)
    println("OUTPUT RESULT IS " + result.min)
  }catch {
    case _ : Exception => println("Exception occurred !!")
      if(maxVal < 0 || minVal < 0) println(1) else println(maxVal + 1)
  }
}
