#! /bin/bash
_PROJECT_HOME="/Users/feiyu/workspace/SparkPlayground/Scala/"
cd ${_PROJECT_HOME}src/main/resources/

# download usb.zip from https://databricks-training.s3.amazonaws.com/getting-started.html
wget "https://s3-us-west-2.amazonaws.com/databricks-meng/usb.zip"
unzip usb.zip

# download graphx data for PageRank implementation
cp -r ${_PROJECT_HOME}src/main/resources/usb/data/graphx ${_PROJECT_HOME}src/main/resources/

rm -rf ${_PROJECT_HOME}src/main/resources/usb*
