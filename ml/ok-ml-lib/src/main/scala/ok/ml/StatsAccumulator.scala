package ok.ml

import org.apache.spark.AccumulatorParam

/**
  * Created by olegklymchuk on 2/20/16.
  */

class StatsAccumulator extends AccumulatorParam[Array[Stats]] {

  override def addAccumulator(t1 : Array[Stats], t2 : Array[Stats]) : Array[Stats] = {

    if(t1.length != t2.length) {
      throw new RuntimeException("Inconsistent array lengths. t1: " + t1.length + "; t2: " + t2.length)
    }

    for(i <- t1.indices) {
      if(t1(i) != null && t2(i) != null) {
        t1(i) = t1(i).reduce(t2(i))
      } else {
        println("TODO")
      }
    }

    t1
  }

  override def addInPlace(r1: Array[Stats], r2: Array[Stats]): Array[Stats] = addAccumulator(r1, r2)

  override def zero(initialValue: Array[Stats]): Array[Stats] = {
    val zero = new Array[Stats](initialValue.length)
    for(i <- zero.indices) {
      zero(i) = new Stats(0)
    }
    zero
  }
}

