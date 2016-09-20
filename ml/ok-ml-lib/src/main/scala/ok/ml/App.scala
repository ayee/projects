package ok.ml

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * @author olegklymchuk
 */
object App {

  def main(args : Array[String]) {

    try {

      if(args.length < 7) {
        println("Not enough parameters provided")
        return
      }

      val master = args(0)
      val path = args(1)
      val delimiter = args(2)
      val omitColumns = args(3).split("\\s*,\\s*").map(_.toInt).distinct
      val recordId = args(4)
      val idColumnIndex = args(5).toInt
      val numNearestNeighbours = args(6).toInt

      val ctx = new SparkContext(new SparkConf().setAppName("K-Nearest Neighbours").setMaster(master))

      val data = ctx.textFile(path)

      val rddInfo = new RDDInfo(delimiter, omitColumns.toSet)

      val normalized = MLUtils.normalize(ctx, data, rddInfo)

      val rddInfoBC = ctx.broadcast(rddInfo)
      val recordIdBC = ctx.broadcast(recordId)
      val idColumnIndexBC = ctx.broadcast(idColumnIndex)

      val target = normalized.filter(s => {
        val splits = s.split(rddInfoBC.value.delimiter)
        splits(idColumnIndexBC.value).equals(recordIdBC.value)
      }).first()

      val result = KNN.knn(ctx, normalized, rddInfo, target, numNearestNeighbours)

      println("KNN for target: " + target)
      result.foreach(println(_))

    } catch {
      case unknown: Throwable => unknown.printStackTrace()
    }

  }

}
