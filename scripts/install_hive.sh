#!/bin/bash

set -ex

NAME=hadoop
DIR=/home/$NAME
VERSION=2.3.6

sudo apt-get install --force-yes -y default-jre wget thrift-compiler
sudo useradd $NAME -m || [ $? -eq 9 ]
cd $DIR
[ -f apache-hive-${VERSION}-bin.tar.gz ] || sudo -H -u $NAME wget "http://apache.cs.utah.edu/hive/hive-${VERSION}/apache-hive-${VERSION}-bin.tar.gz" -O apache-hive-${VERSION}-bin.tar.gz
echo "0b3736edc8d15f01ed649bfce7d74346c35fd57567411e9d0c3f48578f76610d apache-hive-${VERSION}-bin.tar.gz" | sha256sum -c
sudo -H -u $NAME /bin/bash <<EOF
set -ex
tar -xzf apache-hive-${VERSION}-bin.tar.gz --strip 1
if [ -z "$DOCKER_BUILD" ]; then
	rm -rf metastore_db
	$DIR/bin/hdfs dfs -mkdir -p /user/hive/warehouse
	$DIR/bin/hdfs dfs -chmod g+w /user/hive/warehouse
	$DIR/bin/hdfs dfs -mkdir -p /tmp
	$DIR/bin/hdfs dfs -chmod g+w /tmp
	HADOOP_HOME=$DIR bin/schematool -initSchema -dbType derby
fi
EOF
