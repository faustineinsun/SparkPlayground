package com.feiyu.pagerank

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._

/**
 * reference: https://databricks-training.s3.amazonaws.com/graph-analytics-with-graphx.html
 * modified by feiyu
 */
// Define a class to more clearly model the user property
case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

object GraphXDemo {
    def main(args: Array[String]) {

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

        println("Usage: /path/to/spark/bin/spark-submit --driver-memory 2g --class com.feiyu.PageRank.GraphXDemo " +
            "target/scala-2.11/scala_2.11-1.0.jar")

        // set up environment
        val conf = new SparkConf()
            .setAppName("PageRankWiki")
            .set("spark.executor.memory", "2g")
            val sc = new SparkContext(conf)

            val vertexArray = Array(
                (1L, ("Alice", 28)),
                (2L, ("Bob", 27)),
                (3L, ("Charlie", 65)),
                (4L, ("David", 42)),
                (5L, ("Ed", 55)),
                (6L, ("Fran", 50))
            )
            val edgeArray = Array(
                Edge(2L, 1L, 7),
                Edge(2L, 4L, 2),
                Edge(3L, 2L, 4),
                Edge(3L, 6L, 3),
                Edge(4L, 1L, 1),
                Edge(5L, 2L, 2),
                Edge(5L, 3L, 8),
                Edge(5L, 6L, 3)
            )
            val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
            val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
            val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

            println(s"/ndisplay the names of the users that are at least 30 years old")
            // Solution 1
            graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
                case (id, (name, age)) => println(s"$name is $age")
            }
            /*
            // Solution 2
            graph.vertices.filter(v => v._2._2 > 30).collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))

            // Solution 3
            for ((id,(name,age)) <- graph.vertices.filter { case (id,(name,age)) => age > 30 }.collect) {
                println(s"$name is $age")
            }
             */

            println(s"--- who likes who:")
            for (triplet <- graph.triplets.collect) {
                println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
            }

            println(s"--- who likes someone else more than 5 times:")
            for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
                println(s"${triplet.srcAttr._1} loves ${triplet.dstAttr._1}")
            }

            println(s"--- the number of people who like each user:")
            val inDegrees: VertexRDD[Int] = graph.inDegrees
            // Create a user Graph
            val initialUserGraph: Graph[User, Int] = graph.mapVertices{ case (id, (name, age)) => User(name, age, 0, 0) }
            // Fill in the degree information
            val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
                  case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
            }.outerJoinVertices(initialUserGraph.outDegrees) {
                  case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
            }
            for ((id, property) <- userGraph.vertices.collect) {
                  println(s"User $id is called ${property.name} and is liked by ${property.inDeg} people.")
            }

            println(s"--- the names of the users who are liked by the same number of people they like:")
            userGraph.vertices.filter {
                  case (id, u) => u.inDeg == u.outDeg
            }.collect.foreach {
                  case (id, property) => println(property.name)
            }

            println(s"--- the oldest follower for each user:")
            // Find the oldest follower for each user
            val oldestFollower: VertexRDD[(String, Int)] = userGraph.mapReduceTriplets[(String, Int)](
                // For each edge send a message to the destination vertex with the attribute of the source vertex
            edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))),
            // To combine messages take the message for the older follower
            (a, b) => if (a._2 > b._2) a else b)
            userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) => optOldestFollower match {
                case None => s"${user.name} does not have any followers."
                case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
            }
            }.collect.foreach { case (id, str) => println(str) }

            println(s"--- the average follower age of the followers of each user:")
            val averageAge: VertexRDD[Double] = userGraph.mapReduceTriplets[(Int, Double)](
                  // map function returns a tuple of (1, Age)
              edge => Iterator((edge.dstId, (1, edge.srcAttr.age.toDouble))),
                // reduce function combines (sumOfFollowers, sumOfAge)
                  (a, b) => ((a._1 + b._1), (a._2 + b._2))
                    ).mapValues((id, p) => p._2 / p._1)

            // Display the results
            userGraph.vertices.leftJoin(averageAge) { (id, user, optAverageAge) =>
                  optAverageAge match {
                          case None => s"${user.name} does not have any followers."
                              case Some(avgAge) => s"The average age of ${user.name}\'s followers is $avgAge."
                                }
                                }.collect.foreach { case (id, str) => println(str) }

            println(s"--- subgraph that the users in it are all 30 or older.")
            val olderGraph = userGraph.subgraph(vpred = (id, user) => user.age >= 30)
            // compute the connected components
            val cc = olderGraph.connectedComponents

            // display the component id of each user:
            olderGraph.vertices.leftJoin(cc.vertices) {
                  case (id, user, comp) => s"${user.name} is in component ${comp.get}"
                  }.collect.foreach{ case (id, str) => println(str) }

            // clean up
            sc.stop()
    }
}
