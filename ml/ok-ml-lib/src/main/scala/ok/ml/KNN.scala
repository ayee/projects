package ok.ml

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by olegklymchuk on 2/20/16.
  */

object KNN {

  def knn(ctx: SparkContext, normalized: RDD[String], rddInfo: RDDInfo, target: String, k: Integer): Array[(String, Double, String, String)] = {

    val targetFeaturesBC = ctx.broadcast(MLUtils.toDoubleArray(target, rddInfo))
    val rddInfoBC = ctx.broadcast(rddInfo)

    val distances = normalized.map(s => {

      var diffSum = 0.0
      val sampleFeatures = MLUtils.toDoubleArray(s, rddInfoBC.value)
      for(i <- sampleFeatures.indices) {
        val targetFeature = targetFeaturesBC.value.apply(i)
        diffSum += Math.pow(targetFeature - sampleFeatures(i), 2)
      }

      val distance: Double = Math.sqrt(diffSum)

      ("distance:", distance, "record:", s)
    })

    distances.sortBy(_._2).take(k)
  }

}
