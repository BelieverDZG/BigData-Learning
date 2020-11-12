package dzg.scala

import java.text.SimpleDateFormat
import java.util.Date

/**
 * This ability to manipulate functions as values is one of the cornerstone of a
 * very interesting programming paradigm called functional programming.
 *
 * @author BelieverDzg
 * @Date 2020/11/4 18:20
 */
object FunctionalProgramming {

  def main(args: Array[String]): Unit = {
    //    oncePerSecond(timeFlies)
    //使用匿名函数的形式
    //    oncePerSecond(() => {
    //      println("anonymous functions " + new Date())
    //    })
    caseClassAndPatternMatching()
    val ref = new Reference[Int]
    ref.set(100)
    println(ref.get * 2)
  }

  /**
   * 函数式编程案例：每秒打印一下语句
   *
   * @param callback 参数为一个无参且没有返回类型的函数
   */
  def oncePerSecond(callback: () => Unit): Unit = {
    while (true) {
      callback();
      Thread sleep 1000
    }
  }

  def timeFlies(): Unit = {
    println("time flies like an arrow...")
  }

  //Environment表示一个将String转化为Int的函数
  type Environment = String => Int

  def caseClassAndPatternMatching(): Unit = {

    //(x+x)+(7+y)
    val exp: Tree = Sum(Sum(Var("x"), Var("x")), Sum(Const(7), Var("y")))
    //这个符号定义了一个函数，当给定字符串“x”作为参数时，它返回整数5，否则抛出异常。
    val env: Environment = {
      case "x" => 5
      case "y" => 7
    }

    println("expression\t" + exp)
    println("evaluation with x = 5 , y = 7 \t" + eval(exp, env))
    println("derive relative to x: \n" + derive(exp, "x"))
    println("derive relative to y: \n" + derive(exp, "y"))
  }

  /**
   * 该评估函数通过对树t进行模式匹配来工作
   *
   * @param t
   * @param env
   * @return
   */
  def eval(t: Tree, env: Environment): Int = t match {
    case Sum(left, right) => eval(left, env) + eval(right, env)
    case Var(n) => env(n)
    case Const(v) => v
  }

  /**
   * 求导数：
   * 和的导数等于导数的和
   *
   * @param t
   * @param v
   * @return
   */
  def derive(t: Tree, v: String): Tree = t match {
    case Sum(left, right) => Sum(derive(left, v), derive(right, v))
    case Var(n) if (v == n) => Const(1)
    case _ => Const(0)
  }

}

class Complex(real: Double, imaginary: Double) {
  def re(): Double = real

  def im(): Double = imaginary
}

/*
使用case修饰的类和标准类不同：
1)可以不使用new进行创建实例，直接Val("123"),代替new Val("123")
2)自动为构造函数参数定义getter函数
3)提供了方法equals和hashCode的默认定义，它们作用于实例的结构而不是它们的标识
4)可以通过模式匹配来分解这些类的实例
 */
abstract class Tree

case class Sum(left: Tree, right: Tree) extends Tree

case class Var(n: String) extends Tree

case class Const(v: Int) extends Tree

//=====trait demo ==============
trait Ord {
  def <(that: Any): Boolean

  def <=(that: Any): Boolean = (this < that) || (this == that)

  def >(that: Any): Boolean = !(this <= that)

  def >=(that: Any): Boolean = !(this < that)

}

class Date(y: Int, m: Int, d: Int) extends Ord {
  def year = y

  def month = m

  def day = d

  override def toString(): String = year + "-" + month + "-" + day

  override def equals(that: Any): Boolean =
    that.isInstanceOf[Date] && {
      val o = that.asInstanceOf[Date]
      o.day == day && o.month == month && o.year == year
    }


  override def <(that: Any): Boolean = {
    if (!that.isInstanceOf[Date])
      sys.error("cannot compare " + that + " and a Date")

    val o = that.asInstanceOf[Date]
    (year < o.year) ||
      (year == o.year && (month < o.month ||
        (month == o.month && day < o.day)))
  }
}

//===========Genericity泛型==========
class Reference[T] {
  private var contents: T = _

  def set(value: T) {
    contents = value
  }

  def get: T = contents
}
