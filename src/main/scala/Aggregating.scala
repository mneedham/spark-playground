import java.io.File

import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Aggregating {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Aggregation")

    val sc = new SparkContext(conf)

    val input = sc.parallelize(1 to 10)

    input.aggregate((0,0)) (
      (x,y) => (x._1 + y, x._2 + 1),
      (x,y) => (x._1 + y._1,x._2 + y._2))
  }


}
