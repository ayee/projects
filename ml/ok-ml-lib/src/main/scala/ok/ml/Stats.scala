package ok.ml

/**
  * Created by olegklymchuk on 2/20/16.
  */

class Stats extends Serializable {

  var min: Double = Double.NaN
  var max: Double = Double.NaN
  var sum: Double = 0
  var na: Double = 0
  var mean: Double = 0
  var numRecords = 1

  def this(value: Double) {

    this()

    min = value
    max = min
    if(value != Double.NaN) {
      sum = min
    }
  }

  def this(record: String) {

    this()

    val splits = record.split(":")

    if(splits.length == 1) {
      if(!splits(0).isEmpty) {
        min = splits(0).toDouble
        max = min
        sum = min
      } else {
        na = 1
      }
    } else if(splits.length == 4) {
      if(!splits(0).isEmpty) {
        min = splits(0).toDouble
      }
      if(!splits(1).isEmpty) {
        max = splits(1).toDouble
      }
      sum = splits(2).toDouble
      na = splits(3).toDouble
    }
  }

  def reduce(that: Stats): Stats = {

    if(min.equals(Double.NaN)) {
      min = that.min
    } else if(!that.min.equals(Double.NaN)) {
      min = Math.min(min, that.min)
    }

    if(max.equals(Double.NaN)) {
      max = that.max
    } else if(!that.max.equals(Double.NaN)) {
      max = Math.max(max, that.max)
    }

    sum += that.sum
    na += that.na

    numRecords += that.numRecords

    this
  }

  override def toString: String = {

    var str = if(!min.equals(Double.NaN)) min.toString else ""
    str += ":"
    if(!max.equals(Double.NaN)) {
      str += max.toString
    }
    str += ":" + sum + ":" + na

    if(str.contains("NaN")) {
      println(str)
    }
    str
  }

}