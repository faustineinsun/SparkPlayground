#! /bin/bash

PJHOME=/Users/feiyu/workspace/SparkPlayground/Python/
cd $PJHOME
if [ ! -d "dataset" ]; then
    mkdir dataset
fi

cd ${DATADIR}dataset/
wget "http://files.grouplens.org/datasets/movielens/ml-1m.zip"
unzip ml-1m.zip
wget "http://files.grouplens.org/datasets/movielens/ml-10m.zip"
unzip ml-10m.zip
