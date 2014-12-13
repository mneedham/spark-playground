import java.io.File

import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CrimeApp3 {

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

    val initialFile = sc.broadcast(withoutHeader)

    val tasks =
      List(((columns: Array[String]) => Array(columns(5), columns(5), "CrimeType"), ":ID,crimeType,:LABEL", "/tmp/primaryTypes.csv"),
           ((columns: Array[String]) => Array(columns(10), columns(10), "Beat"), ":ID,id,:LABEL", "/tmp/beats.csv"),
           ((columns: Array[String]) => Array(columns(0),columns(0), "Crime", columns(2), columns(6)), ":ID,id,:LABEL,date,description", "/tmp/crimes.csv"),
           ((columns: Array[String]) => Array(columns(0),columns(5), "CRIME_TYPE"), ":START_ID,:END_ID,:TYPE", "/tmp/crimesPrimaryTypes.csv"),
           ((columns: Array[String]) => Array(columns(0),columns(10), "ON_BEAT"), ":START_ID,:END_ID,:TYPE", "/tmp/crimesBeats.csv")
      )
    sc.parallelize(tasks)
      .map(tuple => transformRDD(initialFile.value, tuple._1, tuple._2, tuple._3))
      .foreach(item => generateFile(item._1, item._2, item._3))
  }

  def transformRDD(withoutHeader: RDD[String], fn: Array[String] => Array[String], header: String , fileName:String, separator: String = ",") = {
    (withoutHeader.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)
        fn(columns).mkString(separator)
      })
    }).distinct(), header, fileName)
  }

  def generateFile(rows: RDD[String], header:String, file:String) = {
    FileUtil.fullyDelete(new File(file))

    val tmpFile = "/tmp/" + System.currentTimeMillis() + "-" + file

    rows.saveAsTextFile(tmpFile)
    merge(tmpFile, file, header)
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
