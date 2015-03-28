#! /bin/bash

PJHOME=/Users/feiyu/workspace/SparkPlayground/Python/
SPARKHOME=/Users/feiyu/workspace/sparkOrg/spark-1.3.0-bin-hadoop2.4/
DATADIR=/Users/feiyu/workspace/SparkPlayground/Python/dataset/

cd $PJHOME
${SPARKHOME}bin/spark-submit MovieLensALS.py ${DATADIR}ml-1m/ personalRatings.txt
