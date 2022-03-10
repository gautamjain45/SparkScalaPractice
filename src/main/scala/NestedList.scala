package Demo

object NestedList extends Serializable with App{

  def NestedListFun() : Unit = {
    val all_lists2 = List(1, List(2, 3, 4), List(List(List(5, 6, 7))), List(8, 9), List(10), 11)
    println("Original List:")
    println(all_lists2)
    val flatten_lists2 = flat_List(all_lists2)
    println("Flatten list:")
    println(flatten_lists2)
  }

  def flat_List(ls: List[_]): List[Any] = ls match {
    case Nil => Nil
    case (head: List[_]) :: tail => println(head)
      println(tail)
      flat_List(head) ::: flat_List(tail)
    case head :: tail =>
      println(head)
      println(tail)
      head :: flat_List(tail)
  }
}
