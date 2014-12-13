import java.io.File

import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object WriteToCsv {

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  def main(args: Array[String]) {
    // https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2
    var crimeFile = "/Users/markneedham/Downloads/Crimes_-_2001_to_present.csv"

    val conf = new SparkConf().setAppName("Simple Application")

    val sc = new SparkContext(conf)

    val crimeData = sc.textFile(crimeFile).cache()
    val withoutHeader: RDD[String] = dropHeader(crimeData)

    val file = "/tmp/primaryTypes.csv"
    FileUtil.fullyDelete(new File(file))

    val destinationFile= "/tmp/singlePrimaryTypes.csv"
    FileUtil.fullyDelete(new File(destinationFile))

    val partitions: RDD[(String, Int)] = withoutHeader.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)
        (columns(5), 1)
      })
    })

    val counts = partitions.
      reduceByKey {case (x,y) => x + y}.
      sortBy {case (key, value) => -value}.
      map { case (key, value) => Array(key, value).mkString(",") }


    counts.saveAsTextFile(file)

    merge(file, destinationFile)
  }


  // http://mail-archives.apache.org/mod_mbox/spark-user/201404.mbox/%3CCAEYYnxYuEaie518ODdn-fR7VvD39d71=CgB_Dxw_4COVXgmYYQ@mail.gmail.com%3E
  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }
}
