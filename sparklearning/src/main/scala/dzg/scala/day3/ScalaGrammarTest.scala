package dzg.scala.day3

import java.net.URL
import java.util

/**
 * @author BelieverDzg
 * @date 2020/10/26 19:18
 */
object ScalaGrammarTest {

  def main(args: Array[String]): Unit = {
    stringTest
    test
    println(t(100))
    a
  }

  private def stringTest = {
    var line = "http://bigdata.edu360.cn/laozhang";

    /*    val splits = line.split("[/]")

        val course = splits(2).split("[.]")(0)

        val teacher = splits(3)

        print(teacher + "-" + course)
        */

    val index = line.lastIndexOf("/")

    val teacher = line.substring(index + 1)

    val url = line.substring(0, index)

    val subject = new URL(url).getHost.split("[.]")(0)
    println(teacher + "-" + subject)

  }


  def test(): Unit = {
    val a = 1;
    val bool = a.!=(1)
    println(bool.toString)
  }

  def t(a: Int): Int = {
    if (a < 0) return 0
    var ans = 0;
    for (i <- 0 to a) {
      ans += i
    }
    ans
  }

  def binarySearch(target: Int, arr: Array[Int]): Int = {
    var left = 0
    var right = arr.length
    var tag = false
    while (left <= right) {
      val mid = left + (right - left) / 2
      if (arr(mid) == target) {
        tag = true
        return mid
      } else if (arr(mid) < target) {
        left = mid + 1
      } else {
        right = mid - 1
      }
    }
    if (!tag) return -1
    left
  }

  def a(): Unit = {
    val someInt = Some(1,2,3,4,5)
    println(someInt.get._3)

    val str =
      """123
        |aaa
        |wwww
        |cccc
        |dddd
        |""".stripMargin

    var a = 0;
    val numList = List(1,2,3,4,5,6,7,8,9,10);

    // for 循环
    for( a <- numList
         if a != 3; if a < 8 ){
      println( "Value of a: " + a );
    }

  }

  def m(x: Int) = x + 2


}

class TreeNode(_value: Int = 0, _left: TreeNode = null, _right: TreeNode = null) {
  var value: Int = _value
  var left: TreeNode = _left
  var right: TreeNode = _right
}

