package com.feiyu.util

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 * modified by feiyu
 */
import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]) {
    val localFile = "src/main/resources/sampleWiki.txt"
    val conf = new SparkConf()
        .setAppName("SimpleApp")
        .setJars(List("target/scala-2.11/scala_2.11-1.0.jar"))
        .setMaster("local[4]")
        .setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)
    val localData = sc.textFile(localFile, 2).cache()
    val numAs = localData.filter(line => line.contains("Newton")).count()
    val numBs = localData.filter(line => line.contains("apple")).count()
    println("Lines with Newton: %s, Lines with apple: %s".format(numAs, numBs))
  }
}
