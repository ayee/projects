package ok.ml

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by olegklymchuk on 2/20/16.
  */

object MLUtils {

  def getSummary(ctx: SparkContext, examples: RDD[Array[Double]]): Summary = {

    val statsAccumulator = new StatsAccumulator()
    val zero = statsAccumulator.zero(new Array[Stats](examples.first().length))
    val recordCounter = ctx.accumulator(zero)(statsAccumulator)

    Summary(recordCounter.value(0).numRecords, Vectors.dense(recordCounter.value.map(_.min))
      , Vectors.dense(recordCounter.value.map(_.max))
      , Vectors.dense(recordCounter.value.map(_.sum/recordCounter.value(0).numRecords)))

  }

  def getSummary(ctx: SparkContext, examples: RDD[String], rddInfo: RDDInfo): Summary = {

    var recordCounter = ctx.accumulator(1) //1 since first 'reduce' call consumes first 2 records and subsequent calls consume previous + new one

    val rddInfoBC = ctx.broadcast(rddInfo)

    val s = examples.reduce((a,b) => {

      recordCounter += 1

      val aSplits = a.split(rddInfoBC.value.delimiter)
      val bSplits = b.split(rddInfoBC.value.delimiter)

      if(aSplits.length != bSplits.length) {
        //todo - corrupted record
        throw new Exception("aSplits.length != bSplits.length")
      }

      for(i <- aSplits.indices) {

        if(!rddInfoBC.value.skipColumns.contains(i)) {

          val aStats = new Stats(aSplits(i))
          val bStats = new Stats(bSplits(i))

          val rStats = aStats.reduce(bStats)
          val statsStr = rStats.toString()
          aSplits(i) = statsStr
        }
      }

      aSplits.mkString(rddInfoBC.value.delimiter)
    })

    var buffer = new ArrayBuffer[String]
    val splits = s.split(rddInfo.delimiter)
    for(i <- splits.indices) {
      if(!rddInfo.skipColumns.contains(i)) {
        buffer += splits(i)
      }
    }

    val arr = buffer.toArray

    Summary(recordCounter.value, Vectors.dense(arr.map(s => new Stats(s).min))
      , Vectors.dense(arr.map(s => new Stats(s).max))
      , Vectors.dense(arr.map(s => new Stats(s).sum / recordCounter.value)))

  }

  def normalize(ctx: SparkContext, examples: RDD[String], rddInfo: RDDInfo): RDD[String] = {

    val summary = getSummary(ctx, examples, rddInfo)

    val minBroadcast = ctx.broadcast(summary.min)
    val maxBroadcast = ctx.broadcast(summary.max)
    val rddInfoBC = ctx.broadcast(rddInfo)

    val normalized = examples.map(s => {

      var j = 0
      val splits = s.split(rddInfoBC.value.delimiter)
      for(i <- splits.indices) {
        if(!rddInfoBC.value.skipColumns.contains(i)) {
          val min = minBroadcast.value.apply(j)
          val max = maxBroadcast.value.apply(j)
          splits(i) = ((splits(i).toDouble - min) / (max - min)).toString
          j += 1
        }
      }

      splits.mkString(rddInfoBC.value.delimiter)
    })

    normalized
  }

  def toDoubleArray(s: String, rddInfo: RDDInfo): Array[Double] = {

    var result = ArrayBuffer[Double](0)

    val splits = s.split(rddInfo.delimiter)
    for(i <- splits.indices) {
      if(!rddInfo.skipColumns.contains(i)) {
        result += splits(i).toDouble
      }
    }

    return result.toArray
  }

}
