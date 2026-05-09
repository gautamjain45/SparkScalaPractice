package Leet_Code_Problems.MaxSubSequence

object MaxSubSequence {
  def main(args : Array[String]) : Unit = {
    val nums1 = Array(2,1,3,3)
    val k1 = 2
    maxSubSequence(nums1, k1)
  }

  def maxSubSequence(nums : Array[Int], k : Int) : Array[Int] = {
    val indexedNums = nums.zipWithIndex
    for(indexedNum <- indexedNums){
      println(s"Value is ${indexedNum._1}")
      println(s"Index of value ${indexedNum._2}")
    }
    val topK = indexedNums.sortBy(-_._1).take(k).sortBy(_._2)
    for(values <- topK){
      println(s"Top values are ${values._1}")
      println(s"Index of top values are ${values._2}")
    }
    topK.map(_._1)
  }
}
