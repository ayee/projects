import ok.ml.{MLUtils, KNN, RDDInfo}
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.Test
import org.scalatest.Assertions

/**
  * Created by olegklymchuk on 2/20/16.
  */

class knnTest extends Assertions {

  @Test
  def testKNN() {

    var conf = new SparkConf().setAppName("K-Nearest Neighbours").setMaster("local")
    conf = conf.set("spark.driver.allowMultipleContexts", "true")
    val ctx = new SparkContext(conf)

    val data = ctx.textFile(getClass.getResource("/wisc_bc_data_no_header.csv").getPath)

    val rddInfo = new RDDInfo(",", Set(0, 1))
    val normalized = MLUtils.normalize(ctx, data, rddInfo)

    val samples = normalized.takeSample(false, 20)
    val rddInfoBC = ctx.broadcast(rddInfo)
    samples.foreach(s => {

      val result = KNN.knn(ctx, normalized, rddInfoBC.value, s, 10)

      println("\nKNN for target: " + s)
      result.foreach(println(_))

    })

  }

}
