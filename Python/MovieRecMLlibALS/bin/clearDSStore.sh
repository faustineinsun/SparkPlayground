#! /bin/bash
# clear all the MAC ".DS_Store" in the project dir recursively
PJHOME=/Users/feiyu/workspace/SparkPlayground/
find $PJHOME -name ".DS_Store" -depth -exec rm {} \;
echo ----- Removed all of the ".DS_Store" files from the $PJHOME dir
