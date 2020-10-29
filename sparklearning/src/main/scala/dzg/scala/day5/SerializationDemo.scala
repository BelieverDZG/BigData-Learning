package dzg.scala.day5

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.SparkContext


/**
 * 序列化测试
 *
 * @author BelieverDzg
 * @date 2020/10/28 a9:34
 */
object SerializationDemo {

  def main(args: Array[String]): Unit = {

    /**
     * 1、new一个实例，然后打印其hashcode值
     * 2、在Driver端创建这个实例
     * 3、序列化后发送给Executor，Executor接收后
     * 反序列化，用一个实现了Runnable接口的一个类包装一下，然后丢到线程池中
     */
    val task = new MapTask()
    println(task)
    //        println(task.hashCode())

    val oos = new ObjectOutputStream(new FileOutputStream("./ser"))
    oos.writeObject(task)
    oos.flush()
    oos.close()

    val ois = new ObjectInputStream(new FileInputStream("./ser"))
    val obj: AnyRef = ois.readObject()

    println("反序列化 " + obj)
    println(obj.equals(task))
    ois.close()

    /**
     * 未添加序列化版本号之前
     *
     * 序列化前后对象不相等！！！
     *
     * dzg.scala.day5.MapTask@55f3ddb1
     * 反序列化 dzg.scala.day5.MapTask@71318ec4
     *
     *
     */

  }

}

@SerialVersionUID(-4486938584926174252L)
class MapTask extends Serializable {


  //以后从那里读取数据

  //以后该如何执行，根据RDD的转换关系（调用了哪个方法，传入了什么函数）

  def demo(arr: Array[Int]): Int = {
    arr.length
  }
}
