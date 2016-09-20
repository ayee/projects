package ok.ml

import org.apache.spark.mllib.linalg.Vector

/**
  * Created by olegklymchuk on 2/20/16.
  */

case class Summary(num: Double, min: Vector, max: Vector, mean: Vector) extends Serializable