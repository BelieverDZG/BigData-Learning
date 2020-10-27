package dzg.scala.day5

/**
 * @author BelieverDzg
 * @date 2020/10/26 21:29
 */
object SortRules {

  implicit object OrderingFreshMan extends Ordering[FreshMan] {
    override def compare(x: FreshMan, y: FreshMan): Int = {
      if (x.fv == y.fv){
        x.age - y.age
      }else{
        y.fv - x.fv
      }
    }
  }

}
