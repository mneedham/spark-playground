import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CrimeCountApp {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val crimeFile = "/Users/markneedham/Downloads/Crimes_-_2001_to_present.csv"
    val crimeData = sc.textFile(crimeFile).cache()
    val withoutHeader: RDD[String] = dropHeader(crimeData)

    withoutHeader.mapPartitions(lines => {
      val parser=new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)
        Array(columns(5)).mkString(",")
      })
    }).countByValue().toList.sortBy(-_._2).foreach(println)
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
