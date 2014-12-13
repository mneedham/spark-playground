import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

object PageRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Page Rank")
    val sc = new SparkContext(conf)

    val users  =
      sc.parallelize(Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof"))))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    val ranks = graph.pageRank(0.0001).vertices

    // Write out the final ranks
    val file = "/tmp/ranks"
    FileUtil.fullyDelete(new File(file))

    val singleFile = "/tmp/fullRanks.csv"
    FileUtil.fullyDelete(new File(singleFile))


    ranks.saveAsTextFile(file)

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(file), hdfs, new Path(singleFile), false, hadoopConfig, null)

  }
}
