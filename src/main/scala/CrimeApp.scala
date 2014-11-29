import java.io.File

import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CrimeApp {
  def main(args: Array[String]) {
    val srcPath = "/tmp/shouba6"
    val dstPath = "/tmp/shouba-joined.csv"
    FileUtil.fullyDelete(new File(srcPath))
    FileUtil.fullyDelete(new File(dstPath))

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val crimeFile = "/Users/markneedham/Downloads/Crimes_-_2001_to_present.csv"
    val crimeData = sc.textFile(crimeFile).cache()
    //    dropHeader(crimeData).mapPartitions(pLines).repartition(1).saveAsTextFile(srcPath)
    dropHeader(crimeData).mapPartitions(pLines).saveAsTextFile(srcPath)

    // hadoop's get-merge to join the output files together - http://stackoverflow.com/questions/23527941/how-to-write-to-csv-in-spark
    // http://stackoverflow.com/questions/12911798/hadoop-how-can-i-merge-reducer-outputs-to-a-single-file
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
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

  def pLines(lines:Iterator[String])  ={
    val parser=new CSVParser(',')
    lines.map(line => {
      val columns = parser.parseLine(line)
      Array(columns(0), columns(1), columns(5)).mkString(",")
    })
  }

}
