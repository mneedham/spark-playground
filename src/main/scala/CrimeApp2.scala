import java.io.File

import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CrimeApp2 {

  def merge(srcPath: String, dstPath: String, header: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    MyFileUtil.copyMergeWithHeader(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, header)
  }

  def main(args: Array[String]) {
    var crimeFile = "/Users/markneedham/Downloads/Crimes_-_2001_to_present.csv"

    if(args.length >= 1) {
      crimeFile = args(0)
    }
    println("Using %s".format(crimeFile))

    val conf = new SparkConf().setAppName("Simple Application")

    val sc = new SparkContext(conf)
    // https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2
    val crimeData = sc.textFile(crimeFile).cache()
    val withoutHeader: RDD[String] = dropHeader(crimeData)

    generateFile("/tmp/primaryTypes.csv", withoutHeader, columns => Array(columns(5), columns(5), "CrimeType"), ":ID,crimeType,:LABEL")
    generateFile("/tmp/beats.csv", withoutHeader, columns => Array(columns(10), columns(10), "Beat"), ":ID,id,:LABEL")
    generateFile("/tmp/crimes.csv", withoutHeader, columns => Array(columns(0),columns(0), "Crime", columns(2), columns(6)), ":ID,id,:LABEL,date,description")
    generateFile("/tmp/crimesPrimaryTypes.csv", withoutHeader, columns => Array(columns(0),columns(5), "CRIME_TYPE"), ":START_ID,:END_ID,:TYPE")
    generateFile("/tmp/crimesBeats.csv", withoutHeader, columns => Array(columns(0),columns(10), "ON_BEAT"), ":START_ID,:END_ID,:TYPE")
  }

  def generateFile(file: String, withoutHeader: RDD[String], fn: Array[String] => Array[String], header: String , separator: String = ",") = {
    FileUtil.fullyDelete(new File(file))

    val primaryTypesTmpFile = "/tmp/" + System.currentTimeMillis() + "-" + file
    val primaryTypes: RDD[String] = withoutHeader.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)
        fn(columns).mkString(separator)
      })
    }).distinct()
    primaryTypes.saveAsTextFile(primaryTypesTmpFile)
    merge(primaryTypesTmpFile, file, header)
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
