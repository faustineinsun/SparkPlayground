/**
 * http://spark.apache.org/docs/latest/quick-start.html
 * Created and modified by feiyu on 12/23/14.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val localFile = "src/main/resources/sampleWiki.txt"
    val conf = new SparkConf()
        .setAppName("SimpleApplication")
        .setJars(List("target/scala-2.11/scala_2.11-1.0.jar"))
        .setMaster("local[4]")
        .setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)
    val localData = sc.textFile(localFile, 2).cache()
    val numAs = localData.filter(line => line.contains("a")).count()
    val numBs = localData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}