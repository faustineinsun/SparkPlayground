#! /bin/bash

PJHOME=/Users/feiyu/workspace/SparkPlayground/Scala/
SPARKHOME=/Users/feiyu/workspace/sparkOrg/spark-1.3.0-bin-hadoop2.4/
DATADIR=/Users/feiyu/workspace/SparkPlayground/Scala/src/main/resources/graphx/
cd $PJHOME

${SPARKHOME}bin/spark-submit --driver-memory 2g --class com.feiyu.pagerank.PageRankWiki target/scala-2.11/scala_2.11-1.0.jar ${DATADIR}graphx-wiki-vertices.txt ${DATADIR}graphx-wiki-edges.txt 
