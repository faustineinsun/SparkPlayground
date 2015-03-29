package com.feiyu.pagerank

/**
 * reference: https://databricks-training.s3.amazonaws.com/graph-analytics-with-graphx.html
 * modified by feiyu
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

object PageRankWiki {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length != 2) {
      println("Usage: /path/to/spark/bin/spark-submit --driver-memory 2g --class com.feiyu.PageRank.PageRankWiki " +
        "target/scala-2.11/scala_2.11-1.0.jar /path/to/resources/graphx/graphx-wiki-vertices.txt /path/to/resources/graphx/graphx-wiki-edges.txt")
      sys.exit(1)
    }

      // set up environment
      val conf = new SparkConf()
        .setAppName("PageRankWiki")
        .set("spark.executor.memory", "2g")
      val sc = new SparkContext(conf)

      // Load the Wikipedia Articles
      val articles: RDD[String] = sc.textFile(args(0)) //"src/main/resources/graphx/graphx-wiki-vertices.txt")
      val links: RDD[String] = sc.textFile(args(1)) //"src/main/resources/graphx/graphx-wiki-edges.txt")

      val vertices = articles.map { line =>
        val fields = line.split('\t')
        (fields(0).toLong, fields(1))
      }
      val edges = links.map { line =>
        val fields = line.split('\t')
        Edge(fields(0).toLong, fields(1).toLong, 0)
      }

      val graph = Graph(vertices, edges, "").cache()

      println("vertices count: " + graph.vertices.count)
      println("triplets count: " + graph.triplets.count)

      graph.triplets.take(5).foreach(println(_))

      val prGraph = graph.pageRank(0.001).cache()

      val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
        (v, title, rank) => (rank.getOrElse(0.0), title)
      }

      println("Page rank top 10:");
      titleAndPrGraph.vertices.top(10) {
        Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
      }.foreach(t => println(t._2._2 + ": " + t._2._1))

      // clean up
      sc.stop()
    }
  }
