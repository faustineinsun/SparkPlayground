#! /bin/bash

PJHOME=/Users/feiyu/workspace/SparkPlayground/Scala/
SPARKHOME=/Users/feiyu/workspace/sparkOrg/spark-1.3.0-bin-hadoop2.4/
cd $PJHOME

${SPARKHOME}bin/spark-submit --driver-memory 2g --class com.feiyu.pagerank.GraphXDemo target/scala-2.11/scala_2.11-1.0.jar
